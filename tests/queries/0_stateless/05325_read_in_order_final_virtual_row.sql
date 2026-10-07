-- Tags: no-parallel-replicas

-- Virtual rows with FINAL: the FINAL merges consume the virtual rows of their part streams and
-- announce their own to the merge after the join, so the read in order through an INNER JOIN,
-- which requires a virtual row, applies to FINAL as well.

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 0;
SET query_plan_read_in_order_through_join = 1, max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 0;
SET join_algorithm = 'hash', enable_join_runtime_filters = 0, enable_parallel_replicas = 0;
SET max_threads = 3, max_final_threads = 3, optimize_on_insert = 0, optimize_read_in_reverse_order_final = 1;
SET do_not_merge_across_partitions_select_final = 0;

DROP TABLE IF EXISTS r;
CREATE TABLE r (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r SELECT number * 3 FROM numbers(70);

SELECT '--- ReplacingMergeTree: plan';
DROP TABLE IF EXISTS t_rep;
CREATE TABLE t_rep (k UInt64, ver UInt8, v String) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
INSERT INTO t_rep SELECT number, 1, 'p1' FROM numbers(100);
INSERT INTO t_rep SELECT 50 + number, 2, 'p2' FROM numbers(100);
INSERT INTO t_rep SELECT 100 + number, 3, 'p3' FROM numbers(100);
INSERT INTO t_rep SELECT number * 10, 4, 'p4' FROM numbers(20);

SELECT '-- INNER JOIN';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 7) WHERE e != '';

SELECT '-- INNER JOIN, reverse';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5) WHERE e != '';

SELECT '-- the part streams and the FINAL merge carry virtual rows';
SELECT countIf(explain LIKE '%VirtualRowTransform%') > 0, countIf(explain LIKE '%ReplacingSorted%') > 0
FROM (EXPLAIN PIPELINE SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 7);

SELECT '--- ReplacingMergeTree: results';
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 7;
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5;
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 7;
SELECT l.k, l.v FROM t_rep AS l FINAL LEFT JOIN r ON l.k = r.k WHERE l.k >= 95 ORDER BY l.k LIMIT 7;
SELECT k, v FROM t_rep FINAL ORDER BY k LIMIT 3 OFFSET 48;
SELECT '-- per block';
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 7 SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5 SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 7 SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT k, v FROM t_rep FINAL ORDER BY k LIMIT 3 OFFSET 48 SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT '-- single thread';
SELECT l.k, l.v FROM t_rep AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 7 SETTINGS max_threads = 1, read_in_order_use_virtual_row_per_block = 1;

SELECT '--- ReplacingMergeTree with is_deleted';
DROP TABLE IF EXISTS t_del;
CREATE TABLE t_del (k UInt64, ver UInt8, del UInt8, v String) ENGINE = ReplacingMergeTree(ver, del) ORDER BY k;
INSERT INTO t_del SELECT number, 1, 0, 'p1' FROM numbers(100);
INSERT INTO t_del SELECT number * 3, 2, 1, 'p2' FROM numbers(10);
INSERT INTO t_del SELECT 6 + number * 6, 3, 0, 'p3' FROM numbers(10);
SELECT l.k, l.v FROM t_del AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6;
SELECT l.k, l.v FROM t_del AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '--- CollapsingMergeTree';
DROP TABLE IF EXISTS t_col;
CREATE TABLE t_col (k UInt64, sign Int8, v String) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
INSERT INTO t_col SELECT number, 1, 'a' FROM numbers(100);
INSERT INTO t_col SELECT number * 2, -1, 'a' FROM numbers(50);
INSERT INTO t_col SELECT number * 4, 1, 'b' FROM numbers(25);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_col AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6) WHERE e != '';
SELECT l.k, l.v FROM t_col AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6;
SELECT l.k, l.v FROM t_col AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '--- VersionedCollapsingMergeTree';
DROP TABLE IF EXISTS t_vcol;
CREATE TABLE t_vcol (k UInt64, sign Int8, ver UInt8, v String) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY k;
INSERT INTO t_vcol SELECT number, 1, 1, 'a' FROM numbers(100);
INSERT INTO t_vcol SELECT number * 2, -1, 1, 'a' FROM numbers(50);
INSERT INTO t_vcol SELECT number * 4, 1, 2, 'b' FROM numbers(25);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_vcol AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6) WHERE e != '';
SELECT l.k, l.v FROM t_vcol AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6;
SELECT l.k, l.v FROM t_vcol AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '--- SummingMergeTree';
DROP TABLE IF EXISTS t_sum;
CREATE TABLE t_sum (k UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO t_sum SELECT number, 1 FROM numbers(100);
INSERT INTO t_sum SELECT number, 10 FROM numbers(100);
INSERT INTO t_sum SELECT 50 + number, 100 FROM numbers(100);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.s FROM t_sum AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6) WHERE e != '';
SELECT l.k, l.s FROM t_sum AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6;
SELECT l.k, l.s FROM t_sum AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;
SELECT k, s FROM t_sum FINAL ORDER BY k LIMIT 4 OFFSET 48 SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '--- AggregatingMergeTree';
DROP TABLE IF EXISTS t_agg;
CREATE TABLE t_agg (k UInt64, m SimpleAggregateFunction(max, UInt64), c AggregateFunction(count)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO t_agg SELECT number, 1, countState() FROM numbers(100) GROUP BY number;
INSERT INTO t_agg SELECT number, 10, countState() FROM numbers(100) GROUP BY number;
INSERT INTO t_agg SELECT 50 + number, 5, countState() FROM numbers(100) GROUP BY number;
SELECT l.k, l.m, countMerge(l.c) FROM t_agg AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 GROUP BY l.k, l.m ORDER BY l.k LIMIT 6;
SELECT l.k, l.m FROM t_agg AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6;
SELECT l.k, l.m FROM t_agg AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;

SELECT '--- CoalescingMergeTree';
DROP TABLE IF EXISTS t_coal;
CREATE TABLE t_coal (k UInt64, a Nullable(UInt64), b Nullable(String)) ENGINE = CoalescingMergeTree ORDER BY k;
INSERT INTO t_coal SELECT number, 1, NULL FROM numbers(100);
INSERT INTO t_coal SELECT number, NULL, 'x' FROM numbers(100);
INSERT INTO t_coal SELECT 50 + number, 2, NULL FROM numbers(100);
SELECT l.k, l.a, l.b FROM t_coal AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6;
SELECT l.k, l.a, l.b FROM t_coal AS l FINAL INNER JOIN r ON l.k = r.k WHERE l.k >= 45 ORDER BY l.k LIMIT 6 SETTINGS read_in_order_use_virtual_row_per_block = 1;

-- The virtual row covers only the primary key; the other sorting key columns get the type default,
-- which positions the virtual row correctly only when it is the smallest value of the type.
SELECT '--- a sorting key longer than the primary key, unsigned suffix: virtual row';
DROP TABLE IF EXISTS t_long_key;
CREATE TABLE t_long_key (k UInt64, v UInt32) ENGINE = ReplacingMergeTree ORDER BY (k, v) PRIMARY KEY k;
INSERT INTO t_long_key SELECT number, 1 FROM numbers(10);
INSERT INTO t_long_key SELECT number, 0 FROM numbers(10);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_long_key AS l FINAL LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3) WHERE e != '';
SELECT l.k, l.v FROM t_long_key AS l FINAL INNER JOIN r ON l.k = r.k ORDER BY l.k, l.v LIMIT 4;

SELECT '--- a sorting key longer than the primary key, signed suffix: no virtual row';
DROP TABLE IF EXISTS t_long_key;
CREATE TABLE t_long_key (k UInt64, v Int32) ENGINE = ReplacingMergeTree ORDER BY (k, v) PRIMARY KEY k;
INSERT INTO t_long_key SELECT number, -1 FROM numbers(10);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT l.k, l.v FROM t_long_key AS l FINAL LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3) WHERE e != '';

SELECT '--- GraphiteMergeTree: no virtual row';
DROP TABLE IF EXISTS t_graphite;
CREATE TABLE t_graphite (Path String, Time DateTime, Value Float64, Version UInt32, k UInt64 ALIAS Version)
    ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
INSERT INTO t_graphite SELECT 'one_min.x', toDateTime('2020-01-01 00:00:00') + number, number, 1 FROM numbers(10);
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT Path, Time FROM t_graphite FINAL ORDER BY Path, Time LIMIT 3) WHERE e != '';

DROP TABLE t_rep;
DROP TABLE t_del;
DROP TABLE t_col;
DROP TABLE t_vcol;
DROP TABLE t_sum;
DROP TABLE t_agg;
DROP TABLE t_coal;
DROP TABLE t_long_key;
DROP TABLE t_graphite;
DROP TABLE r;
