# docker build -t daisy/clickhouse-stateful-tests-runner .
ARG TAG=latest
FROM yandex/clickhouse-test-base AS datasets

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9

ENV DATASETS="hits visits"
COPY s3downloader /usr/bin/s3downloader
RUN s3downloader --dataset-names hits visits --preload


FROM daisy/clickhouse-statelest-tests-runner:${TAG}
COPY --from=datasets /tmp /tmp
ENV DATASETS="hits visits"
COPY s3downloader /usr/bin/s3downloader
COPY dockerd-entrypoint.sh /usr/bin/dockerd-entrypoint.sh
entrypoint ["dockerd-entrypoint.sh"]
