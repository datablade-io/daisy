import json

import pytest
from helpers.cluster import ClickHouseCluster
import time

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

    ### create local MergeTree table
    instance1.query("""
    CREATE TABLE local_table1(
        _time DateTime64,
        n UInt64
        )
    ENGINE = MergeTree() ORDER BY _time
    """
                   )

    ### create DistributedMergeTree table
    instances = [instance1, instance2, instance3, instance4]
    # create 4 shard 1 replica
    for i in range(0, 4):
        instances[i].query(f"""
        CREATE TABLE default.test_table_4_1 (
            `col_1` Int32,
            `_time` DateTime64(3) DEFAULT now64(3)
        ) ENGINE = DistributedMergeTree(1, 4, rand())
        PARTITION BY toYYYYMMDD(_time)
        ORDER BY (toYYYYMMDD(_time), col_1)
        SETTINGS subtype='tabular', shard={i}
        """)
    # create 3 shard 1 replica
    for i in range(0, 3):
        instances[i].query(f"""
        CREATE TABLE default.test_table_3_1 (
            `col_1` Int32,
            `_time` DateTime64(3) DEFAULT now64(3)
        ) ENGINE = DistributedMergeTree(1, 3, rand())
        PARTITION BY toYYYYMMDD(_time)
        ORDER BY (toYYYYMMDD(_time), col_1)
        SETTINGS subtype='tabular', shard={i}
        """)
    time.sleep(2)


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
    resp = instance1.http_request(method="GET", url=f"dae/v1/tablestats/test_table_4_1?local={local}")
    assert 200 == resp.status_code
    for key in keys:
        assert key in resp.text


### API URL FORMAT TEST
@pytest.mark.parametrize("code, url_path", [
        (400, 'dae/v1/tablestats'),
        (400, 'dae/v1/tablestats/'),
        (404, 'dae/v1/tablestats/test_table_4_1/'),
        (200, 'dae/v1/tablestats/test_table_4_1')
    ])
@pytest.mark.parametrize("url_parameter", [
        '?local=true',  # means !0
        '?local=1',     # means !0
        '?local=abc',   # means !0
        '?local=false', # means 0
        '?local=0',     # means 0
        ''
    ])
def test_tablestats_api_url_format(code, url_path, url_parameter):
    resp = instance1.http_request(method="GET", url=f"{url_path}{url_parameter}")
    assert code == resp.status_code


### API ERROR TEST
# err_status:  0-SUCCESS  1-ERROR  2-ONLY ONE NODE ERROR
@pytest.mark.parametrize("err_status, local, table", [
        (1, 'true', 'local_table1'),    # MergeTree no support
        (1, 'false', 'local_table1'),
        (1, 'true', 'abc'),             # Table not exist
        (1, 'false', 'abc'),
        (2, 'true', 'test_table_3_1'),  # in one node, has not a local table
        (0, 'false', 'test_table_3_1')
    ])
def test_tablestats_api_error(err_status, local, table):
    resp = instance1.http_request(method="GET", url=f"dae/v1/tablestats/{table}?local={local}")
    if err_status == 2: # ONLY ONE NODE ERROR
        err_count = 0 if 200 == resp.status_code else 1
        err_count += 0 if 200 == instance2.http_request(method="GET", url=f"dae/v1/tablestats/{table}?local={local}").status_code else 1
        err_count += 0 if 200 == instance3.http_request(method="GET", url=f"dae/v1/tablestats/{table}?local={local}").status_code else 1
        err_count += 0 if 200 == instance4.http_request(method="GET", url=f"dae/v1/tablestats/{table}?local={local}").status_code else 1
        assert err_count == 1
    elif err_status == 1:
        assert 400 == resp.status_code
    elif err_status == 0:
        assert 200 == resp.status_code
