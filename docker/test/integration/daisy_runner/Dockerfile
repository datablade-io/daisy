# docker build -t daisy/clickhouse-integration-tests-runner .
FROM yandex/clickhouse-integration-tests-runner
RUN python3 -m pip install -U pip
RUN python3 -m pip install -U setuptools
COPY requirements.txt /
RUN while read requirement; do python3 -m pip install -U $requirement; done < /requirements.txt

COPY dockerd-entrypoint.sh /usr/local/bin/dockerd-entrypoint.sh
COPY compose/ /compose/
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["sh", "-c", "pytest $PYTEST_OPTS"]
