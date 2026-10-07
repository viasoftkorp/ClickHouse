-- Tags: no-random-merge-tree-settings

-- SELECT ... FINAL must not lose rows when the read-in-order virtual row is enabled: the FINAL
-- merge skips the virtual rows of its part streams and only positions the streams by them.
-- ORDER BY with a LIMIT is required: without it the read-in-order plan is not built at all.

DROP TABLE IF EXISTS t_vrow_select_final;

CREATE TABLE t_vrow_select_final (x UInt64, y UInt64)
ENGINE = ReplacingMergeTree(y)
ORDER BY x;

SYSTEM STOP MERGES t_vrow_select_final;

INSERT INTO t_vrow_select_final SELECT number, 1 FROM numbers(8192);
INSERT INTO t_vrow_select_final SELECT number, 2 FROM numbers(8192);

-- Every SETTINGS clause below sits on the outermost statement: one attached to an inner subquery
-- loses to the same setting arriving as a client option.
SELECT count(), sum(y)
FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 20000)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0;

SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 3
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0;

SELECT count()
FROM (SELECT x FROM t_vrow_select_final ORDER BY x LIMIT 20000)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0;

-- The row counts above stay correct whenever the virtual row is never attempted, so assert that
-- the in-order FINAL read does build the virtual row conversion, like the same fixture without
-- FINAL. `ReadType:` is matched whitespace-insensitively: the label prints as `Read type: ` or
-- `ReadType: ` depending on `pretty`.
SELECT countIf(replaceAll(explain, ' ', '') ILIKE '%ReadType:InOrder%') > 0
   AND countIf(explain ILIKE '%Virtual row conversions%') > 0
FROM (EXPLAIN actions = 1
      SELECT count(), sum(y) FROM (SELECT x, y FROM t_vrow_select_final FINAL ORDER BY x LIMIT 20000)
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0);

SELECT countIf(replaceAll(explain, ' ', '') ILIKE '%ReadType:InOrder%') > 0
   AND countIf(explain ILIKE '%Virtual row conversions%') > 0
FROM (EXPLAIN actions = 1
      SELECT count() FROM (SELECT x FROM t_vrow_select_final ORDER BY x LIMIT 20000)
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, enable_parallel_replicas = 0);

DROP TABLE t_vrow_select_final;
