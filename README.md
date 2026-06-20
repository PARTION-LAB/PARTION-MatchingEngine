# PARTION Matching Engine

C++ Kafka matching engine for PARTION trading orders.

## Local Run

Run Kafka and the matching engine together:

```bash
docker compose up --build
```

The engine consumes order commands from `partion.order.commands` and publishes trade events to
`partion.trade.events`.

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers. Defaults to `localhost:9092`.
- `KAFKA_GROUP_ID`: Kafka consumer group id. Defaults to `partion-matching-engine`.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```
