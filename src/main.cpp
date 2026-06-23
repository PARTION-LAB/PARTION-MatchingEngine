#include <csignal>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {

constexpr const char* kOrderTopic = "partion.order.commands";
constexpr const char* kTradeTopic = "partion.trade.events";

volatile std::sig_atomic_t running = 1;

void handle_signal(int) {
    running = 0;
}

std::string env_or(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || std::string(value).empty() ? fallback : value;
}

void set_conf(rd_kafka_conf_t* conf, const char* name, const std::string& value) {
    char error[512];
    if (rd_kafka_conf_set(conf, name, value.c_str(), error, sizeof(error)) != RD_KAFKA_CONF_OK) {
        throw std::runtime_error(std::string("Kafka config failed: ") + error);
    }
}

long long parse_money_cents(const json& value) {
    if (value.is_string()) {
        std::string text = value.get<std::string>();
        bool negative = !text.empty() && text[0] == '-';
        if (negative) {
            text.erase(0, 1);
        }

        auto dot = text.find('.');
        std::string whole = dot == std::string::npos ? text : text.substr(0, dot);
        std::string fraction = dot == std::string::npos ? "00" : text.substr(dot + 1);
        fraction.resize(2, '0');
        long long cents = std::stoll(whole) * 100 + std::stoll(fraction.substr(0, 2));
        return negative ? -cents : cents;
    }

    if (value.is_number_integer()) {
        return value.get<long long>() * 100;
    }

    return static_cast<long long>(std::llround(value.get<double>() * 100.0));
}

std::string format_money(long long cents) {
    std::ostringstream out;
    out << (cents / 100) << "." << std::setw(2) << std::setfill('0') << std::llabs(cents % 100);
    return out.str();
}

struct Order {
    long long order_id;
    long long member_id;
    long long product_id;
    std::string side;
    std::string order_method;
    long long price_cents;
    long long original_quantity;
    long long remaining_quantity;
    long long sequence;
};

struct Trade {
    std::string event_id;
    long long product_id;
    long long buy_order_id;
    long long sell_order_id;
    long long price_cents;
    long long quantity;
};

struct OrderCommand {
    std::string command_type;
    long long order_id;
    long long product_id;
    std::optional<Order> order;
};

class KafkaProducer {
public:
    explicit KafkaProducer(const std::string& brokers) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        set_conf(conf, "bootstrap.servers", brokers);

        char error[512];
        producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        if (producer_ == nullptr) {
            throw std::runtime_error(std::string("Producer creation failed: ") + error);
        }
    }

    ~KafkaProducer() {
        if (producer_ != nullptr) {
            rd_kafka_flush(producer_, 5000);
            rd_kafka_destroy(producer_);
        }
    }

    void send_trade(const Trade& trade) {
        json payload = {
                {"eventType", "TRADE_EXECUTED"},
                {"eventId", trade.event_id},
                {"productId", trade.product_id},
                {"buyOrderId", trade.buy_order_id},
                {"sellOrderId", trade.sell_order_id},
                {"price", format_money(trade.price_cents)},
                {"quantity", trade.quantity},
                {"occurredAt", now_millis()}
        };

        std::string message = payload.dump();
        std::string key = std::to_string(trade.product_id);

        rd_kafka_resp_err_t error = rd_kafka_producev(
                producer_,
                RD_KAFKA_V_TOPIC(kTradeTopic),
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
                RD_KAFKA_V_VALUE(const_cast<char*>(message.data()), message.size()),
                RD_KAFKA_V_END
        );

        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::cerr << "Failed to produce trade event: " << rd_kafka_err2str(error) << '\n';
        }

        rd_kafka_poll(producer_, 0);
    }

    void send_order_execution_result(const Order& order) {
        long long filled_quantity = order.original_quantity - order.remaining_quantity;
        long long canceled_quantity = order.remaining_quantity;
        std::string final_status = "CANCELED";

        if (filled_quantity == order.original_quantity) {
            final_status = "FILLED";
        } else if (filled_quantity > 0) {
            final_status = "PARTIALLY_FILLED";
        }

        json payload = {
                {"eventType", "ORDER_EXECUTION_RESULT"},
                {"eventId", std::to_string(order.product_id) + "-" + std::to_string(order.order_id) + "-result"},
                {"orderId", order.order_id},
                {"productId", order.product_id},
                {"side", order.side},
                {"orderMethod", order.order_method},
                {"requestedQuantity", order.original_quantity},
                {"filledQuantity", filled_quantity},
                {"canceledQuantity", canceled_quantity},
                {"remainingQuantity", 0},
                {"finalStatus", final_status},
                {"occurredAt", now_millis()}
        };

        std::string message = payload.dump();
        std::string key = std::to_string(order.product_id);

        rd_kafka_resp_err_t error = rd_kafka_producev(
                producer_,
                RD_KAFKA_V_TOPIC(kTradeTopic),
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
                RD_KAFKA_V_VALUE(const_cast<char*>(message.data()), message.size()),
                RD_KAFKA_V_END
        );

        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::cerr << "Failed to produce order execution result event: " << rd_kafka_err2str(error) << '\n';
        }

        rd_kafka_poll(producer_, 0);
    }

private:
    static long long now_millis() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    rd_kafka_t* producer_{nullptr};
};

class OrderBook {
public:
    explicit OrderBook(KafkaProducer& producer) : producer_(producer) {
    }

    void accept(Order order) {
        if (order.order_method != "LIMIT" && order.order_method != "MARKET") {
            std::cerr << "Unknown order method. orderId=" << order.order_id
                      << " orderMethod=" << order.order_method << '\n';
            return;
        }

        if (order.side == "BUY") {
            match_buy(order);
        } else if (order.side == "SELL") {
            match_sell(order);
        } else {
            std::cerr << "Unknown order side. orderId=" << order.order_id << '\n';
            return;
        }

        if (order.order_method == "MARKET") {
            producer_.send_order_execution_result(order);
            std::cout << "finalized market orderId=" << order.order_id
                      << " productId=" << order.product_id
                      << " filled=" << (order.original_quantity - order.remaining_quantity)
                      << " canceled=" << order.remaining_quantity << '\n';
        }
    }

    bool cancel(long long order_id) {
        return remove_from_book(buy_book_, order_id) || remove_from_book(sell_book_, order_id);
    }

private:
    template <typename Book>
    bool remove_from_book(Book& book, long long order_id) {
        for (auto level = book.begin(); level != book.end();) {
            auto& orders = level->second;
            bool removed = false;

            for (auto order = orders.begin(); order != orders.end();) {
                if (order->order_id == order_id) {
                    order = orders.erase(order);
                    removed = true;
                } else {
                    ++order;
                }
            }

            if (orders.empty()) {
                level = book.erase(level);
            } else {
                ++level;
            }

            if (removed) {
                return true;
            }
        }

        return false;
    }

    void match_buy(Order& incoming) {
        while (incoming.remaining_quantity > 0 && !sell_book_.empty()) {
            auto best = sell_book_.begin();
            if (best->first > incoming.price_cents) {
                break;
            }

            match_level(incoming, best->second, best->first);
            if (best->second.empty()) {
                sell_book_.erase(best);
            }
        }

        if (incoming.remaining_quantity > 0 && incoming.order_method == "LIMIT") {
            buy_book_[incoming.price_cents].push_back(incoming);
        }
    }

    void match_sell(Order& incoming) {
        while (incoming.remaining_quantity > 0 && !buy_book_.empty()) {
            auto best = buy_book_.begin();
            if (best->first < incoming.price_cents) {
                break;
            }

            match_level(incoming, best->second, best->first);
            if (best->second.empty()) {
                buy_book_.erase(best);
            }
        }

        if (incoming.remaining_quantity > 0 && incoming.order_method == "LIMIT") {
            sell_book_[incoming.price_cents].push_back(incoming);
        }
    }

    void match_level(Order& incoming, std::deque<Order>& resting_orders, long long trade_price) {
        while (incoming.remaining_quantity > 0 && !resting_orders.empty()) {
            Order& resting = resting_orders.front();
            long long quantity = std::min(incoming.remaining_quantity, resting.remaining_quantity);

            Trade trade = make_trade(incoming, resting, trade_price, quantity);
            producer_.send_trade(trade);

            incoming.remaining_quantity -= quantity;
            resting.remaining_quantity -= quantity;

            std::cout << "matched eventId=" << trade.event_id
                      << " productId=" << trade.product_id
                      << " price=" << format_money(trade.price_cents)
                      << " quantity=" << trade.quantity << '\n';

            if (resting.remaining_quantity == 0) {
                resting_orders.pop_front();
            }
        }
    }

    Trade make_trade(const Order& incoming, const Order& resting, long long trade_price, long long quantity) {
        const Order& buy = incoming.side == "BUY" ? incoming : resting;
        const Order& sell = incoming.side == "SELL" ? incoming : resting;
        ++trade_sequence_;

        return Trade{
                std::to_string(buy.product_id) + "-" + std::to_string(trade_sequence_),
                buy.product_id,
                buy.order_id,
                sell.order_id,
                trade_price,
                quantity
        };
    }

    KafkaProducer& producer_;
    std::map<long long, std::deque<Order>, std::greater<>> buy_book_;
    std::map<long long, std::deque<Order>> sell_book_;
    long long trade_sequence_{0};
};

class MatchingEngine {
public:
    explicit MatchingEngine(KafkaProducer& producer) : producer_(producer) {
    }

    void accept(Order order) {
        auto [iterator, inserted] = books_.try_emplace(order.product_id, producer_);
        iterator->second.accept(order);
    }

    bool cancel(long long product_id, long long order_id) {
        auto iterator = books_.find(product_id);
        if (iterator == books_.end()) {
            return false;
        }

        return iterator->second.cancel(order_id);
    }

private:
    KafkaProducer& producer_;
    std::unordered_map<long long, OrderBook> books_;
};

OrderCommand parse_order_command(const std::string& message, long long sequence) {
    json payload = json::parse(message);
    std::string command_type = payload.value("commandType", std::string("NEW_ORDER"));
    long long order_id = payload.at("orderId").get<long long>();
    long long product_id = payload.at("productId").get<long long>();

    if (command_type == "CANCEL_ORDER") {
        return OrderCommand{
                command_type,
                order_id,
                product_id,
                std::nullopt
        };
    }

    long long quantity = payload.at("quantity").get<long long>();
    std::string order_method = payload.value("orderMethod", std::string("LIMIT"));

    return OrderCommand{
            command_type,
            order_id,
            product_id,
            Order{
                    order_id,
                    payload.at("memberId").get<long long>(),
                    product_id,
                    payload.at("side").get<std::string>(),
                    order_method,
                    parse_money_cents(payload.at("price")),
                    quantity,
                    quantity,
                    sequence
            }
    };
}

rd_kafka_t* create_consumer(const std::string& brokers) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    set_conf(conf, "bootstrap.servers", brokers);
    set_conf(conf, "group.id", env_or("KAFKA_GROUP_ID", "partion-matching-engine"));
    set_conf(conf, "auto.offset.reset", "earliest");
    set_conf(conf, "enable.auto.commit", "true");

    char error[512];
    rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof(error));
    if (consumer == nullptr) {
        throw std::runtime_error(std::string("Consumer creation failed: ") + error);
    }

    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, kOrderTopic, RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t subscribe_error = rd_kafka_subscribe(consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (subscribe_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(consumer);
        throw std::runtime_error(std::string("Subscribe failed: ") + rd_kafka_err2str(subscribe_error));
    }

    return consumer;
}

} // namespace

int main() {
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    try {
        std::string brokers = env_or("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        KafkaProducer producer(brokers);
        MatchingEngine engine(producer);
        rd_kafka_t* consumer = create_consumer(brokers);

        std::cout << "Partion matching engine started. brokers=" << brokers << '\n';
        long long sequence = 0;

        while (running) {
            rd_kafka_message_t* message = rd_kafka_consumer_poll(consumer, 1000);
            if (message == nullptr) {
                continue;
            }

            if (message->err) {
                if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    std::cerr << "Kafka consume error: " << rd_kafka_message_errstr(message) << '\n';
                }
                rd_kafka_message_destroy(message);
                continue;
            }

            try {
                std::string payload(static_cast<char*>(message->payload), message->len);
                OrderCommand command = parse_order_command(payload, ++sequence);

                if (command.command_type == "CANCEL_ORDER") {
                    bool removed = engine.cancel(command.product_id, command.order_id);
                    std::cout << "received cancel orderId=" << command.order_id
                              << " productId=" << command.product_id
                              << " removed=" << (removed ? "true" : "false") << '\n';
                    rd_kafka_message_destroy(message);
                    continue;
                }

                if (command.command_type != "NEW_ORDER") {
                    std::cerr << "Unknown order command type. commandType=" << command.command_type
                              << " orderId=" << command.order_id << '\n';
                    rd_kafka_message_destroy(message);
                    continue;
                }

                Order order = command.order.value();
                std::cout << "received orderId=" << order.order_id
                          << " productId=" << order.product_id
                          << " side=" << order.side
                          << " orderMethod=" << order.order_method
                          << " price=" << format_money(order.price_cents)
                          << " quantity=" << order.remaining_quantity << '\n';
                engine.accept(order);
            } catch (const std::exception& exception) {
                std::cerr << "Failed to process order message: " << exception.what() << '\n';
            }

            rd_kafka_message_destroy(message);
        }

        rd_kafka_consumer_close(consumer);
        rd_kafka_destroy(consumer);
        std::cout << "Partion matching engine stopped.\n";
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << exception.what() << '\n';
        return 1;
    }
}
