# docker build -t yandex/clickhouse-python-bottle .
# Helper docker container to run python bottle apps

FROM python:3

RUN python -m pip install -U pip
RUN python -m pip config set global.index-url http://mirrors.aliyun.com/pypi/simple
RUN python -m pip config set install.trusted-host mirrors.aliyun.com

RUN python -m pip install bottle
