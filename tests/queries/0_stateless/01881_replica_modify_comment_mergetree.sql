DROP TABLE IF EXISTS t_replicatedmt_1 ON CLUSTER test_cluster_two_shards;

CREATE TABLE t_replicatedmt_1 ON CLUSTER test_cluster_two_shards (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t_replicatedmt_1', '{replica}')  ORDER BY id COMMENT 'ReplicatedMergeTree table';

SHOW CREATE TABLE t_replicatedmt_1;
SELECT name, comment FROM system.tables WHERE name = 't_replicatedmt_1';

ALTER TABLE t_replicatedmt_1 MODIFY COMMENT 'This is ReplicatedMergeTree table';

SHOW CREATE TABLE t_replicatedmt_1;
SELECT name, comment FROM system.tables WHERE name = 't_replicatedmt_1';

ALTER TABLE t_replicatedmt_1 MODIFY COMMENT '';

SHOW CREATE TABLE t_replicatedmt_1;
SELECT name, comment FROM system.tables WHERE name = 't_replicatedmt_1';

ALTER TABLE t_replicatedmt_1 MODIFY COMMENT 'This is ReplicatedMergeTree table';

SHOW CREATE TABLE t_replicatedmt_1;
SELECT name, comment FROM system.tables WHERE name = 't_replicatedmt_1';

DROP TABLE t_replicatedmt_1 ON CLUSTER test_cluster_two_shards;