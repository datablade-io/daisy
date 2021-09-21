import json

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance1 = cluster.add_instance('node1',
                                main_configs=['configs/daisy_on_kafka_master1.xml'],
                                with_kafka=True
                                )
instance2 = cluster.add_instance('node2',
                                main_configs=['configs/daisy_on_kafka_node2.xml'],
                                with_kafka=True
                                )
instance3 = cluster.add_instance('node3',
                                main_configs=['configs/daisy_on_kafka_node3.xml'],
                                with_kafka=True
                                )
instance4 = cluster.add_instance('node4',
                                main_configs=['configs/daisy_on_kafka_node4.xml'],
                                with_kafka=True
                                )

def prepare_data():
    print("prepare data")

    ### create and insert local MergeTree table for 4 nodes
    instance1.query("""
    CREATE TABLE local_table1(
        _time DateTime64,
        n UInt64
        )
    ENGINE = MergeTree() ORDER BY _time
    """
                   )
    instance2.query("""
    CREATE TABLE local_table2(
        _time DateTime64,
        n UInt64
        )
    ENGINE = MergeTree() ORDER BY _time
    """
                   )
    instance3.query("""
    CREATE TABLE local_table3(
        _time DateTime64,
        n UInt64
        )
    ENGINE = MergeTree() ORDER BY _time
    """
                   )
    instance4.query("""
    CREATE TABLE local_table4(
        _time DateTime64,
        n UInt64
        )
    ENGINE = MergeTree() ORDER BY _time
    """
                   )
    for d in range(0, 10):
        for i in range(10):
            instance1.query("""
            INSERT INTO local_table1(_time, n) VALUES('2021-01-2{date}', 123)
            """.format(date=d))
    for d in range(0, 10):
        for i in range(10):
            instance2.query("""
            INSERT INTO local_table2(_time, n) VALUES('2021-01-2{date}', 123)
            """.format(date=d))
    for d in range(0, 10):
        for i in range(10):
            instance3.query("""
            INSERT INTO local_table3(_time, n) VALUES('2021-01-2{date}', 123)
            """.format(date=d))
    for d in range(0, 10):
        for i in range(10):
            instance4.query("""
            INSERT INTO local_table4(_time, n) VALUES('2021-01-2{date}', 123)
            """.format(date=d))

    ### create and ingest DistributedMergeTree table
    instance1.http_request(method="POST", url="dae/v1/ddl/tables", data=json.dumps("""
    {
        "name": "test_table_2_2",
        "shards": 2,
        "replication_factor": 2,
        "shard_by_expression": "rand()",
        "columns": [
            {
                "name": "col_1",
                "type": "Int32",
                "nullable": false
            },
            {
                "name": "col_2",
                "type": "Int32",
                "nullable": false
            }
        ],
        "order_by_expression": "col_1",
        "partition_by_granularity": "D"
    }
    """))
    instance1.http_request(method="POST", url="dae/v1/ingest/tables/test_table_2_2", data=json.dumps("""
    {
        "columns": [
            "_time",
            "col_1",
            "col_2"
        ],
        "data":
        [
            ["2021-08-01", 1, 2],
            ["2021-08-02", 3, 2]
        ]
    }
    """))



@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    print("setup node")
    try:
        cluster.start()
        prepare_data()
        yield cluster

    finally:
        cluster.shutdown()


### API HAPPYPATH TEST
@pytest.mark.parametrize("local, keys", [
    ('false', [
        "request_id",
        "data",
            "name",
            "streaming", 
                "topic",
                "partitions",
                    "partition",
                    "end_offset",
                    "consumer_groups",
                        "group_id",
                        "member_id",
                        "offset",
                        "committed_offset",
            "local",
                "shards",
                    "shard",
                    "replicas",
                        "total_rows",
                        "total_bytes",
                        "parts",
                            "active_count",
                            "inactive_count",
                        "current_sn",
                        "startup_max_committed_sn",
                        "current_max_committed_sn"
    ]),
    ('true', [
        "request_id",
        "data",
            "name",
            "shard",
            "streaming",
                "topic",
                "partition",
                "app_offset",
                "committed_offset",
                "end_offset",
                "group_id",
                "member_id",
            "local",
                "total_rows",
                "total_bytes",
                "parts",
                    "active_count",
                    "inactive_count",
                "current_sn",
                "startup_max_committed_sn",
                "current_max_committed_sn"
    ])
])
def test_tablestats_api_main(local, keys):
    res = instance1.http_request(method="GET", url=f"dae/v1/tablestats/test_table_2_2?local={local}")
    print(res.text)
    for key in keys:
        assert key in res.text
    

# ### API URL FORMAT TEST
# @pytest.mark.parametrize("url_path", ['dae/v1/tablestats', 'dae/v1/tablestats/', 'dae/v1/tablestats/test_table_2_2'])
# @pytest.mark.parametrize("url_parameter", ['?local=true', '?local=false', '?local=abc', '?', ''])
# def test_tablestats_api_url_format(url_path, url_parameter):
#     res = instance1.http_request(method="GET", url=f"{url_path}{url_parameter}")
#     print(res)

# ### API ERROR TEST
# @pytest.mark.parametrize("table", ['local_table1', 'local_table2', 'local_table3', 'local_table4', 'abc'])
# @pytest.mark.parametrize("local", ['true', 'false'])
# def test_tablestats_api_error(table, local):
#     res = instance1.http_request(method="GET", url=f"dae/v1/tablestats/{table}?local={local}")
#     print(res)