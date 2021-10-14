# docker build -t daisy/tabs-data:iot-1 .
FROM golang:latest as builder

# Install Time Series Benchmark suite
RUN go get github.com/timescale/tsbs \
    && cd $GOPATH/src/github.com/timescale/tsbs \
    && make

RUN tsbs_generate_data --use-case="iot" --seed=123 --scale=4000 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-02T00:00:00Z" \
    --log-interval="20m" --format="timescaledb" \
    | gzip > /tmp/timescaledb-data.gz