# PARTION Matching Engine

C++ Kafka matching engine for PARTION trading orders.

## Local Run

Run Kafka and the matching engine together:

```bash
docker compose up --build
```

The engine consumes order commands from `partion.order.commands` and publishes trade events to
`partion.trade.events`.

## Kafka Message Contract

### Order command topic

Topic: `partion.order.commands`

Create order command:

```json
{
  "commandType": "CREATE",
  "orderId": 1,
  "memberId": 1,
  "productId": 10,
  "side": "BUY",
  "price": "10000.00",
  "quantity": 5
}
```

Cancel order command:

```json
{
  "commandType": "CANCEL",
  "orderId": 1,
  "productId": 10
}
```

For backward compatibility, commands without `commandType` are treated as `CREATE`.

### Trade event topic

Topic: `partion.trade.events`

Trade event:

```json
{
  "eventId": "10-1-2-1780000000000-1",
  "productId": 10,
  "buyOrderId": 1,
  "sellOrderId": 2,
  "price": "10000.00",
  "quantity": 5,
  "occurredAt": 1780000000000
}
```

`eventId` is generated as `{productId}-{buyOrderId}-{sellOrderId}-{occurredAt}-{sequence}`.

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers. Defaults to `localhost:9092`.
- `KAFKA_GROUP_ID`: Kafka consumer group id. Defaults to `partion-matching-engine`.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```
