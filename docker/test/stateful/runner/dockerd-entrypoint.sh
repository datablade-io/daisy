#!/bin/bash

set -e -x

export LLVM_PROFILE_FILE='/tests_output/coverage_reports/stateful_tests_clickhouse_%h_%p_%m.profraw'

# Start server
counter=0
until clickhouse client --query "SELECT 1"
do
    if [ "$counter" -gt 120 ]
    then
        echo "Cannot start clickhouse server"
        cat /var/log/clickhouse-server/stdout.log
        tail -n1000 /var/log/clickhouse-server/stderr.log
        tail -n1000 /var/log/clickhouse-server/clickhouse-server.log
        break
    fi
    clickhouse server --config=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log --daemon
    sleep 0.5
    counter=$((counter + 1))
done

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
# already preload
s3downloader --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse client --query "SHOW DATABASES"
clickhouse client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
clickhouse server --config=/etc/clickhouse-server1/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log --daemon restart

# Wait for server to start accepting connections
for _ in {1..120}; do
    clickhouse client --query "SELECT 1" && break
    sleep 1
done

clickhouse client --query "SHOW TABLES FROM datasets"

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    clickhouse client --query "CREATE DATABASE test ON CLUSTER 'test_cluster_database_replicated'
        ENGINE=Replicated('/test/clickhouse/db/test', '{shard}', '{replica}')"

    clickhouse client --query "CREATE TABLE test.hits AS datasets.hits_v1"
    clickhouse client --query "CREATE TABLE test.visits AS datasets.visits_v1"

    clickhouse client --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1"
    clickhouse client --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1"

    clickhouse client --query "DROP TABLE datasets.hits_v1"
    clickhouse client --query "DROP TABLE datasets.visits_v1"

    MAX_RUN_TIME=$((MAX_RUN_TIME < 9000 ? MAX_RUN_TIME : 9000))  # min(MAX_RUN_TIME, 2.5 hours)
    MAX_RUN_TIME=$((MAX_RUN_TIME != 0 ? MAX_RUN_TIME : 9000))    # set to 2.5 hours if 0 (unlimited)
else
    clickhouse client --query "CREATE DATABASE test"
    clickhouse client --query "SHOW TABLES FROM test"
    clickhouse client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
    clickhouse client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
fi

clickhouse client --query "SHOW TABLES FROM test"
clickhouse client --query "SELECT count() FROM test.hits"
clickhouse client --query "SELECT count() FROM test.visits"

clickhouse-test -q /usr/share/clickhouse-test/queries -j$(($(nproc)/2)) --testname --shard --zookeeper --no-stateless --hung-check --print-time $@  2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /tests_output/${TEST_TAG}_stateful_test_result.txt

