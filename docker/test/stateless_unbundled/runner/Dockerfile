# docker build -t daisy/clickhouse-statelest-tests-runner .
FROM yandex/clickhouse-stateless-unbundled-test
COPY dockerd-entrypoint.sh /usr/bin/dockerd-entrypoint.sh
RUN mkdir -p /var/log/clickhouse-server/
VOLUME /var/log/clickhouse-server/
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD []
