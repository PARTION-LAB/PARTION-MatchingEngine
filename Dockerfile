FROM ubuntu:24.04 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       cmake \
       librdkafka-dev \
       nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY CMakeLists.txt .
COPY src ./src

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel

FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/build/partion-matching-engine /usr/local/bin/partion-matching-engine

ENTRYPOINT ["partion-matching-engine"]
