# docker build -t daisy/clickhouse-statelest-tests-runner .
FROM yandex/clickhouse-stateless-unbundled-test
COPY dockerd-entrypoint.sh /usr/bin/dockerd-entrypoint.sh
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD []
