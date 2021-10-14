# docker build -t daisy/tabs-data:iot-1 .
FROM golang:latest as tsbs

# Install Time Series Benchmark suite
RUN mkdir /src \
    && cd /src \
    && git clone https://github.com/timescale/tsbs.git \
    && cd tsbs \
    && make

FROM yandex/clickhouse-server as ch

ENV PATH="/tsbs/${PATH}"

RUN mkdir /tsbs && mkdir /data

COPY --from=tsbs /src/tsbs/bin /tsbs
COPY --from=tsbs /src/tsbs/scripts /tsbs
COPY ./export_benchmark_data.sh /tsbs

RUN service clickhouse-server start \
    && sleep 10 \
    && FORMATS=clickhouse generate_data.sh \
    && FORMATS=clickhouse generate_queries.sh \
    && load/load_clickhouse.sh \
    && export_benchmark_data.sh \
    service clickhouse-server stop

FROM ubuntu:20.04

COPY --from=ch /data / 
