#!/bin/bash

set -e -x
export LLVM_PROFILE_FILE='/tests_output/coverage_reports/statelest_tests_clickhouse_%5m.profraw'

# Start server
counter=0
until clickhouse client --query "SELECT 1"
do
    if [ "$counter" -gt 30 ]
    then
        echo "Cannot start clickhouse server"
        cat /var/log/clickhouse-server/stdout.log
        tail -n1000 /var/log/clickhouse-server/stderr.log
        tail -n1000 /var/log/clickhouse-server/clickhouse-server.log
        break
    fi
    clickhouse server --config=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log --daemon
    sleep 2
    counter=$((counter + 1))
done

clickhouse-test -q /clickhouse-tests-env/queries -j$(($(nproc)/4)) --no-stateful --testname --shard --zookeeper --print-time $@ 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /tests_output/${TEST_TAG}_statelest_test_result.txt
