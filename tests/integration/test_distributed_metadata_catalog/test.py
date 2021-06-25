import json
import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
                             main_configs=['configs/remote_servers.xml', 'configs/config_ddl.xml', 'configs/kafka.xml'],
                             with_kafka=True, with_zookeeper=True, ipv4_address='10.5.0.11')

node2 = cluster.add_instance('node2',
                             main_configs=['configs/remote_servers.xml', 'configs/config.xml', 'configs/kafka.xml'],
                             with_kafka=True, with_zookeeper=True, ipv4_address='10.5.0.12')
node3 = cluster.add_instance('node3',
                             main_configs=['configs/remote_servers.xml', 'configs/config.xml', 'configs/kafka.xml'],
                             with_kafka=True, with_zookeeper=True, ipv4_address='10.5.0.13')

nodes = [node1, node2, node3]

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("query, status", [
    (
            {
                "name": "demo_tb_0001",
                "shards": 1,
                "replication_factor": 1,
                "shard_by_expression": "0",
                "columns": [{"name": "col_1", "type": "Int32", "default": "1", "compression_codec": "ZSTD"},
                            {"name": "col_2", "type": "Float32", "nullable": False},
                            {"name": "col_3", "type": "DateTime", "nullable": False, "default": "now()",
                             "compression_codec": "ZSTD"}, {"name": "col_4", "type": "String",
                                                            "skipping_index_expression": "INDEX a (col_1 * col_2, col_4) TYPE minmax GRANULARITY 3"}],
                "order_by_expression": "(col_3, col_2)",
                "partition_by_granularity": "M",
                "partition_by_expression": "toYYYYMM(col_3)",
                "ttl_expression": "col_3 + INTERVAL 1 MONTH",
                "_time_column": "col_3"
            },
            {
                "status": 200
            }
    ),
    (
            {
                "name": "demo_tb_0002",
                "shards": 1,
                "replication_factor": 1,
                "shard_by_expression": "0",
                "columns": [{"name": "col_1", "type": "Int32", "default": "1", "compression_codec": "ZSTD"},
                            {"name": "col_2", "type": "Float32", "nullable": False},
                            {"name": "col_3", "type": "DateTime", "nullable": False, "default": "now()",
                             "compression_codec": "ZSTD"}, {"name": "col_4", "type": "String",
                                                            "skipping_index_expression": "INDEX a (col_1 * col_2, col_4) TYPE minmax GRANULARITY 3"}],
                "order_by_expression": "(col_3, col_2)",
                "partition_by_granularity": "M",
                "partition_by_expression": "toYYYYMM(col_3)",
                "ttl_expression": "col_3 + INTERVAL 1 MONTH",
                "_time_column": "col_3"
            },
            {
                "status": 200
            }
    )
])
def test_create_table_catalog_case(query, status):

    time.sleep(10)  # wait for __system_ddls create

    resp = node1.http_request(method="POST", url="dae/v1/ddl/tables", data=json.dumps(query))
    result = json.loads(resp.content)
    print(result)
    assert resp.status_code == status['status']

    time.sleep(10)  # wait for ddl servcie sendRequest

    resp1 = node1.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result1 = json.loads(resp1.content)
    print(result1)

    resp2 = node2.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result2 = json.loads(resp2.content)
    print(result2)

    resp3 = node3.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result3 = json.loads(resp3.content)
    print(result3)

    del result1['request_id']
    del result2['request_id']
    del result3['request_id']

    assert result1 == result2 == result3
    assert query['name'] in json.dumps(result1)



@pytest.mark.parametrize("talbe", ["demo_tb_0001", "demo_tb_0002"])
def test_delete_table_catalog_case(talbe):

    resp = node1.http_request(method="DELETE", url="dae/v1/ddl/tables/" + talbe, data="")
    result = json.loads(resp.content)
    print(result)
    assert resp.status_code == 200

    resp1 = node1.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result1 = json.loads(resp1.content)
    print(result1)

    resp2 = node2.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result2 = json.loads(resp2.content)
    print(result2)

    resp3 = node3.http_request(method="GET", url="dae/v1/ddl/tables", data="")
    result3 = json.loads(resp3.content)
    print(result3)

    del result1['request_id']
    del result2['request_id']
    del result3['request_id']

    assert result1 == result2 == result3
