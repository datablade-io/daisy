DROP TABLE IF EXISTS t_shard1_all ON CLUSTER test_cluster_two_shards;
DROP TABLE IF EXISTS t_shard1_local ON CLUSTER test_cluster_two_shards;

CREATE TABLE t_shard1_local ON CLUSTER test_cluster_two_shards (id UInt64) ENGINE = MergeTree() ORDER BY id COMMENT 'MergeTree table';
CREATE TABLE t_shard1_all ON CLUSTER test_cluster_two_shards (id UInt64) ENGINE = Distributed(test_cluster_two_shards, default, t_shard1_local, rand()) COMMENT 'Distributed table for MergeTree table';

SHOW CREATE TABLE t_shard1_local;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_local';
SHOW CREATE TABLE t_shard1_all;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_all';

ALTER TABLE t_shard1_all ON CLUSTER test_cluster_two_shards MODIFY COMMENT 'This is Distributed table for MergeTree table';

SHOW CREATE TABLE t_shard1_local;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_local';
SHOW CREATE TABLE t_shard1_all;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_all';

ALTER TABLE t_shard1_local ON CLUSTER test_cluster_two_shards MODIFY COMMENT 'This is MergeTree table';

SHOW CREATE TABLE t_shard1_local;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_local';
SHOW CREATE TABLE t_shard1_all;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_all';

ALTER TABLE t_shard1_all ON CLUSTER test_cluster_two_shards MODIFY COMMENT '';
ALTER TABLE t_shard1_local ON CLUSTER test_cluster_two_shards MODIFY COMMENT '';

SHOW CREATE TABLE t_shard1_local;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_local';
SHOW CREATE TABLE t_shard1_all;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_all';

ALTER TABLE t_shard1_all ON CLUSTER test_cluster_two_shards MODIFY COMMENT 'This is Distributed table for MergeTree table';
ALTER TABLE t_shard1_local ON CLUSTER test_cluster_two_shards MODIFY COMMENT 'This is MergeTree table';

SHOW CREATE TABLE t_shard1_local;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_local';
SHOW CREATE TABLE t_shard1_all;
SELECT name, comment FROM system.tables WHERE name = 't_shard1_all';

DROP TABLE t_shard1_all ON CLUSTER test_cluster_two_shards;
DROP TABLE t_shard1_local ON CLUSTER test_cluster_two_shards;