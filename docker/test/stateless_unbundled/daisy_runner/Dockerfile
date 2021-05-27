# docker build -t daisy/clickhouse-statelest-tests-runner .
FROM yandex/clickhouse-stateless-unbundled-test
COPY dockerd-entrypoint.sh /usr/bin/dockerd-entrypoint.sh
RUN mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client \
    && chmod ugo+Xrw -R /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client
VOLUME ['/var/log/clickhouse-server/', '/var/lib/clickhouse']
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD []
