import json
import pdb
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml', 'configs/config_ddl.xml', 'configs/kafka.xml'], with_kafka=True, with_zookeeper=True, ipv4_address='10.5.0.11')

node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml', 'configs/config.xml', 'configs/kafka.xml'], with_kafka=True, with_zookeeper=True,  ipv4_address='10.5.0.12')
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml', 'configs/config.xml', 'configs/kafka.xml'], with_kafka=True, with_zookeeper=True,  ipv4_address='10.5.0.13')

nodes = [node1, node2, node3]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for i, node in enumerate([node1]):
            node.query("CREATE DATABASE testdb")
            node.query(
                '''CREATE TABLE testdb.test_table(id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/test/test_table1', '{}') ORDER BY id;'''.format(
                    i))
        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("query", [({"name:":"test"})])


def test_ingest_api_basic_case(query):
    pdb.set_trace()
    query = "{\"name\": \"test\"}"
    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    resp = node1.http_request(method="POST", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    query =""
    resp = node1.http_request(method="GET", url="dae/v1/ddl/databases" , data=query)
    result = json.loads(resp.content)
    print(result)

    query =""
    resp = node1.http_request(method="DELETE", url="dae/v1/ddl/databases/test" , data=query)
    result = json.loads(resp.content)
    print(result)