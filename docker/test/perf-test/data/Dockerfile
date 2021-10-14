# docker build -t daisy/tabs-data:latest .
ARG VAR_SCALE
ARG VAR_SEED
ARG VAR_TS_START
ARG VAR_TS_END
ARG VAR_USE_CASE
ARG VAR_LOG_INTERVAL
ARG VAR_MAX_DATA_POINTS

FROM golang:latest as tsbs

# Install Time Series Benchmark suite
RUN mkdir /src \
    && cd /src \
    && git clone https://github.com/timescale/tsbs.git \
    && cd tsbs \
    && make

FROM yandex/clickhouse-server:latest as ch

ARG VAR_SCALE
ARG VAR_SEED
ARG VAR_TS_START
ARG VAR_TS_END
ARG VAR_USE_CASE
ARG VAR_LOG_INTERVAL
ARG VAR_MAX_DATA_POINTS

ENV PATH="/tsbs:$PATH"
ENV SCALE=$VAR_SCALE
ENV SEED=$VAR_SEED
ENV TS_START=$VAR_TS_START
ENV TS_END=$VAR_TS_END
ENV USE_CASE=$VAR_USE_CASE
ENV LOG_INTERVAL=$VAR_LOG_INTERVAL
ENV MAX_DATA_POINTS=$VAR_MAX_DATA_POINTS

RUN mkdir /tsbs && mkdir /data

COPY --from=tsbs /src/tsbs/bin /tsbs
COPY --from=tsbs /src/tsbs/scripts /tsbs
COPY --from=tsbs /src/tsbs/scripts/load /tsbs
COPY ./export_benchmark_data.sh /tsbs

RUN echo $PATH \
    && sed -i '/--port=/d' /tsbs/load_clickhouse.sh \
    && service clickhouse-server start \service clickhouse-server start \
    && sleep 10 \
    && FORMATS=clickhouse generate_data.sh \
    && FORMATS=clickhouse generate_queries.sh \
    && load_clickhouse.sh \
    && export_benchmark_data.sh \
    service clickhouse-server stop

FROM ubuntu:20.04

COPY --from=ch /data /data
