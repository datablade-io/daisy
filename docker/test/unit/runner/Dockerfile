# docker build -t daisy/clickhouse-unit-tests-runner .
FROM yandex/clickhouse-stateless-test

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install gdb

COPY dockerd-entrypoint.sh /usr/bin/dockerd-entrypoint.sh
COPY process_unit_tests_result.py /usr/bin/process_unit_tests_result.py
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD []
