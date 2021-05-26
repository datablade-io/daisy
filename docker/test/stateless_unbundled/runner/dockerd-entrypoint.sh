#!/bin/bash

set -e -x
export LLVM_PROFILE_FILE='/tests_output/coverage_reports/statelest_tests_clickhouse_%h_%p_%m.profraw'

clickhouse server --config-file=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log --daemon

clickhouse-test -q /usr/share/clickhouse-test/queries -j$(($(nproc)/2)) --no-stateful --testname --shard --zookeeper --print-time $@ 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /tests_output/${TEST_TAG}_statelest_test_result.txt
