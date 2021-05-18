#!/bin/bash

set -e -x
export PATH=$PATH:/programs
mkdir -p /var/log/clickhouse-server/

clickhouse server --config-file=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log --daemon

clickhouse-test -q /queries -j$(($(nproc)/2)) --no-stateful --testname --shard --zookeeper --print-time $@ 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /tests_output/${TEST_TAG}_statelest_test_result.txt
