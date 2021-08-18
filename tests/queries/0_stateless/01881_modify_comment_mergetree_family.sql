DROP TABLE IF EXISTS t_mergetree;
DROP TABLE IF EXISTS t_mergetree_nocomment;
DROP TABLE IF EXISTS t_replacingmt;
DROP TABLE IF EXISTS t_summingmt;
DROP TABLE IF EXISTS t_aggregatingmt;
DROP TABLE IF EXISTS t_collapsingmt;
DROP TABLE IF EXISTS t_versioncollapsingmt;
DROP TABLE IF EXISTS t_graphitemt;

CREATE TABLE t_mergetree (id UInt64) ENGINE MergeTree() ORDER BY id COMMENT 'MergeTree table';
CREATE TABLE t_mergetree_nocomment (id UInt64) ENGINE MergeTree() ORDER BY id;
CREATE TABLE t_replacingmt (id UInt64) ENGINE ReplacingMergeTree() ORDER BY id COMMENT 'ReplacingMergeTree table';
CREATE TABLE t_summingmt (id UInt64) ENGINE SummingMergeTree() ORDER BY id COMMENT 'SummingMergeTree table';
CREATE TABLE t_aggregatingmt (id UInt64) ENGINE AggregatingMergeTree() ORDER BY id COMMENT 'AggregatingMergeTree table';
CREATE TABLE t_collapsingmt (id UInt64, sign Int8) ENGINE CollapsingMergeTree(sign) ORDER BY id COMMENT 'CollapsingMergeTree table';
CREATE TABLE t_versioncollapsingmt ( id UInt64, sign Int8, version UInt8 ) ENGINE VersionedCollapsingMergeTree(sign, version) ORDER BY id COMMENT 'VersionedCollapsingMergeTree table';
CREATE TABLE t_graphitemt (path String, time DateTime, value UInt32, version UInt32, id UInt32)  ENGINE = GraphiteMergeTree('graphite_rollup') PARTITION BY time ORDER BY intHash32(id) SAMPLE BY intHash32(id) COMMENT 'GraphiteMergeTree table';

SHOW CREATE TABLE t_mergetree;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree';
SHOW CREATE TABLE t_mergetree_nocomment;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree_nocomment';
SHOW CREATE TABLE t_replacingmt;
SELECT name, comment FROM system.tables WHERE name = 't_replacingmt';
SHOW CREATE TABLE t_summingmt;
SELECT name, comment FROM system.tables WHERE name = 't_summingmt';
SHOW CREATE TABLE t_aggregatingmt;
SELECT name, comment FROM system.tables WHERE name = 't_aggregatingmt';
SHOW CREATE TABLE t_collapsingmt;
SELECT name, comment FROM system.tables WHERE name = 't_collapsingmt';
SHOW CREATE TABLE t_versioncollapsingmt;
SELECT name, comment FROM system.tables WHERE name = 't_versioncollapsingmt';
SHOW CREATE TABLE t_graphitemt;
SELECT name, comment FROM system.tables WHERE name = 't_graphitemt';

ALTER TABLE t_mergetree MODIFY COMMENT 'This is MergeTree table';
ALTER TABLE t_mergetree_nocomment MODIFY COMMENT 'This is no comment MergeTree table';
ALTER TABLE t_replacingmt MODIFY COMMENT 'This is ReplacingMergeTree table';
ALTER TABLE t_summingmt MODIFY COMMENT 'This is SummingMergeTree table';
ALTER TABLE t_aggregatingmt MODIFY COMMENT 'This is AggregatingMergeTree table';
ALTER TABLE t_collapsingmt MODIFY COMMENT 'This is CollapsingMergeTree table';
ALTER TABLE t_versioncollapsingmt MODIFY COMMENT 'This is VersionedCollapsingMergeTree table';
ALTER TABLE t_graphitemt MODIFY COMMENT 'This is GraphiteMergeTree table';

SHOW CREATE TABLE t_mergetree;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree';
SHOW CREATE TABLE t_mergetree_nocomment;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree_nocomment';
SHOW CREATE TABLE t_replacingmt;
SELECT name, comment FROM system.tables WHERE name = 't_replacingmt';
SHOW CREATE TABLE t_summingmt;
SELECT name, comment FROM system.tables WHERE name = 't_summingmt';
SHOW CREATE TABLE t_aggregatingmt;
SELECT name, comment FROM system.tables WHERE name = 't_aggregatingmt';
SHOW CREATE TABLE t_collapsingmt;
SELECT name, comment FROM system.tables WHERE name = 't_collapsingmt';
SHOW CREATE TABLE t_versioncollapsingmt;
SELECT name, comment FROM system.tables WHERE name = 't_versioncollapsingmt';
SHOW CREATE TABLE t_graphitemt;
SELECT name, comment FROM system.tables WHERE name = 't_graphitemt';

ALTER TABLE t_mergetree MODIFY COMMENT '';
SHOW CREATE TABLE t_mergetree;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree';

ALTER TABLE t_mergetree MODIFY COMMENT 'This is MergeTree table';
SHOW CREATE TABLE t_mergetree;
SELECT name, comment FROM system.tables WHERE name = 't_mergetree';

DROP TABLE t_mergetree;
DROP TABLE t_mergetree_nocomment;
DROP TABLE t_replacingmt;
DROP TABLE t_summingmt;
DROP TABLE t_aggregatingmt;
DROP TABLE t_collapsingmt;
DROP TABLE t_versioncollapsingmt;
DROP TABLE t_graphitemt;