# docker build -t daisy/tabs-data:iot-1 .
FROM golang:latest as tsbs

# Install Time Series Benchmark suite
RUN mkdir /src \
    && cd /src \
    && git clone https://github.com/timescale/tsbs.git \
    && cd tsbs \
    && make

FROM yandex/clickhouse-server as ch

RUN mkdir /tsbs
COPY --from=tsbs /src/tsbs/bin /tsbs
COPY --from=tsbs /src/tsbs/scripts /tsbs


