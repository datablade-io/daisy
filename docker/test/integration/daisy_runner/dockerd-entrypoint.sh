#!/bin/bash
set -e

mkdir -p /etc/docker/
cat > /etc/docker/daemon.json << EOF
{
    "ipv6": true,
    "fixed-cidr-v6": "fd00::/8",
    "ip-forward": true,
    "insecure-registries" : ["dockerhub-proxy.sas.yp-c.yandex.net:5000"],
    "registry-mirrors" : ["http://dockerhub-proxy.sas.yp-c.yandex.net:5000"]
}
EOF

dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2375 &>/var/log/somefile &

set +e
reties=0
rereties=0  # multipled retrties
while true; do
    docker info &>/dev/null && break
    reties=$((reties+1))
    if [[ $reties -ge 100 ]]; then # 10 sec max
        if [[ $rereties -ge 3 ]]; then
            echo "Can't start docker daemon, timeout exceeded." >&2
            exit 1;
        else
            echo "start docker daemon, timeout exceeded, retry ..."
            kill -9 `cat /var/run/docker.pid` || true
            rm -rf /var/run/docker.pid /var/run/docker/containerd/containerd.pid || true # try delete existed pid to continue
            dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2375 &>/var/log/somefile &
            rereties=$(($rereties+1))
            reties=0;
        fi
    fi
    sleep 0.1
done

    
set -e

echo "Start tests"
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/usr/bin/clickhouse
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/usr/bin/clickhouse
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/etc/clickhouse-server/
export CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH=/usr/bin/clickhouse-odbc-bridge
export CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH=/usr/bin/clickhouse-library-bridge
export DOCKER_COMPOSE_DIR=/compose/

export DOCKER_MYSQL_GOLANG_CLIENT_TAG=${DOCKER_MYSQL_GOLANG_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JAVA_CLIENT_TAG=${DOCKER_MYSQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JS_CLIENT_TAG=${DOCKER_MYSQL_JS_CLIENT_TAG:=latest}
export DOCKER_MYSQL_PHP_CLIENT_TAG=${DOCKER_MYSQL_PHP_CLIENT_TAG:=latest}
export DOCKER_POSTGRESQL_JAVA_CLIENT_TAG=${DOCKER_POSTGRESQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_KERBEROS_KDC_TAG=${DOCKER_KERBEROS_KDC_TAG:=latest}

cd /clickhouse-tests-env/integration
exec "$@"
