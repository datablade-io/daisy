#!/bin/bash

set -x
export LLVM_PROFILE_FILE='/test_output/coverage_reports/clickhouse_%h_%p_%m.profraw'
gdb -q  -ex 'set print inferior-events off' -ex 'set confirm off' -ex 'set print thread-events off' -ex run -ex bt -ex quit --args ./unit_tests_dbms $@ | tee test_output/${TEST_TAG}_unit_tests_result.txt
./process_unit_tests_result.py  || echo -e "failure\tCannot parse results" > /test_output/${TEST_TAG}_unit_tests_check_status.tsv
