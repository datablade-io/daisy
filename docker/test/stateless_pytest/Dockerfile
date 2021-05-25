# docker build -t yandex/clickhouse-stateless-pytest .
FROM yandex/clickhouse-test-base

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        brotli \
        netcat-openbsd \
        zstd

RUN python3 -m pip install -U pip
RUN python3 -m pip config set global.index-url http://mirrors.aliyun.com/pypi/simple
RUN python3 -m pip config set install.trusted-host mirrors.aliyun.com

RUN python3 -m pip install \
    wheel \
    pytest \
    pytest-html \
    pytest-json \
    pytest-randomly \
    pytest-rerunfailures \
    pytest-timeout \
    pytest-xdist \
    pandas \
    numpy \
    scipy

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    python3 -m pytest /usr/share/clickhouse-test/queries -n $(nproc) --reruns=1 --timeout=600 --json=test_output/report.json --html=test_output/report.html --self-contained-html
