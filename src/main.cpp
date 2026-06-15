#include <csignal>
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
    long long price_cents;
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
        if (order.side == "BUY") {
            match_buy(order);
        } else if (order.side == "SELL") {
            match_sell(order);
        } else {
            std::cerr << "Unknown order side. orderId=" << order.order_id << '\n';
        }
    }

private:
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

        if (incoming.remaining_quantity > 0) {
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

        if (incoming.remaining_quantity > 0) {
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

private:
    KafkaProducer& producer_;
    std::unordered_map<long long, OrderBook> books_;
};

Order parse_order(const std::string& message, long long sequence) {
    json payload = json::parse(message);
    return Order{
            payload.at("orderId").get<long long>(),
            payload.at("memberId").get<long long>(),
            payload.at("productId").get<long long>(),
            payload.at("side").get<std::string>(),
            parse_money_cents(payload.at("price")),
            payload.at("quantity").get<long long>(),
            sequence
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
                Order order = parse_order(payload, ++sequence);
                std::cout << "received orderId=" << order.order_id
                          << " productId=" << order.product_id
                          << " side=" << order.side
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
