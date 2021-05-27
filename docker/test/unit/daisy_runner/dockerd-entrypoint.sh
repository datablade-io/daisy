#!/bin/bash

set -x
export LLVM_PROFILE_FILE='/tests_output/coverage_reports/unit_tests_clickhouse_%h_%p_%m.profraw'
gdb -q  -ex 'set print inferior-events off' -ex 'set confirm off' -ex 'set print thread-events off' -ex run -ex bt -ex quit --args unit_tests_dbms $@ | tee /tests_output/${TEST_TAG}_unit_tests_result.txt
process_unit_tests_result.py  || echo -e "failure\tCannot parse results" > /tests_output/${TEST_TAG}_unit_tests_check_status.tsv
