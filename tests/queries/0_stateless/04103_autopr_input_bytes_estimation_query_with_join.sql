-- Tags: stateful

SET use_uncompressed_cache=0;
SET use_query_condition_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- External sort spills data to disk and reads it back, which inflates `ReadCompressedBytes`
SET max_bytes_before_external_sort=0, max_bytes_ratio_before_external_sort=0;

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

-- The stateless-test randomizer flips `query_plan_join_swap_table` between `'auto'` and `'false'`,
-- which can move which side of an INNER/RIGHT join becomes the parallelized one. The queries
-- below assume the user-written orientation reaches `findReadingStep`; pin the setting off so
-- the legacy swap doesn't reorient us out of that. Individual queries override locally when they
-- specifically want to exercise the swap (e.g. `04103_query_6`).
SET query_plan_join_swap_table='false';

set enable_filesystem_cache=1;

SET parallel_replicas_prefer_local_join=1;

-- Use a tiny auxiliary `MergeTree` table as the right side of the JOIN. The autopr statistics
-- only cover the parallelized (left) side, whereas `ReadCompressedBytes` in `system.query_log`
-- aggregates reads from every source step, so any non-trivial right side would skew the ratio
-- even when the estimate itself is correct. A minimal right side keeps the two quantities
-- directly comparable.
DROP TABLE IF EXISTS autopr_join_right_small;
DROP TABLE IF EXISTS autopr_join_right_small_2;
CREATE TABLE autopr_join_right_small (UserID UInt64) ENGINE = MergeTree ORDER BY UserID AS SELECT number FROM numbers(1000);
CREATE TABLE autopr_join_right_small_2 (UserID UInt64) ENGINE = MergeTree ORDER BY UserID AS SELECT number FROM numbers(1000);

-- `INNER JOIN` of `test.hits` with the tiny right-side table.
-- With `parallel_replicas_prefer_local_join=1`, the left side is the parallelized side, so the
-- reported `RuntimeDataflowStatisticsInputBytes` should approximate the bytes read from `test.hits`.
SELECT count() FROM test.hits AS t1 INNER JOIN autopr_join_right_small AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_1';

-- Same JOIN with a filter on the left side. `URL` is not part of the primary key, so this filter
-- does not prune granules via PK index analysis; the full left side is still read from disk and
-- therefore `ReadCompressedBytes` stays comparable to the collected statistics.
SELECT count() FROM test.hits AS t1 INNER JOIN autopr_join_right_small AS t2 USING (UserID) WHERE t1.URL LIKE '%com%' FORMAT Null SETTINGS log_comment='04103_query_2';

-- `LEFT JOIN` with aggregation on top.
SELECT t1.CounterID, count() AS c FROM test.hits AS t1 LEFT JOIN autopr_join_right_small AS t2 USING (UserID) GROUP BY t1.CounterID ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='04103_query_3', max_block_size=65409;

-- Plain `LEFT JOIN`. Left side is parallelized (same convention as `INNER JOIN`), so statistics
-- are collected at the `test.hits` read.
SELECT count() FROM test.hits AS t1 LEFT JOIN autopr_join_right_small AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_4';

-- `RIGHT JOIN`: the parallelized side is the right one. `autopr_join_right_small` is placed on the
-- left so that `test.hits` (the larger table) ends up on the parallelized side, exercising the
-- `children.at(1)` branch in `findReadingStep` that handles the `RIGHT JOIN` convention.
SELECT count() FROM autopr_join_right_small AS t1 RIGHT JOIN test.hits AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_5';

-- `INNER JOIN` with the small table on the SQL-left side. The test pins
-- `query_plan_join_swap_table='true'` and a non-zero `query_plan_optimize_join_order_limit`
-- against the randomizer (which could otherwise force one or the other off), so the DP
-- join-order optimizer actually runs and swaps the sides. After the swap `test.hits` ends up
-- on the parallelized (left) child of the physical `JoinStep`, making the collected
-- statistics directly comparable to `ReadCompressedBytes`.
SELECT count() FROM autopr_join_right_small AS t1 INNER JOIN test.hits AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_6', query_plan_join_swap_table='true', query_plan_optimize_join_order_limit=10;

-- Three-way left-deep join: `(test.hits JOIN small1) JOIN small2`. With small auxiliaries on both
-- right sides the parallelized side stays as `test.hits` after each join; `findReadingStep` walks
-- through two nested `JoinStep` nodes, descending into `children.at(0)` each time before reaching
-- the `ReadFromMergeTree` for `test.hits`.
SELECT count() FROM test.hits AS t1 INNER JOIN autopr_join_right_small AS t2 USING (UserID) INNER JOIN autopr_join_right_small_2 AS t3 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_7';

-- Three-way nested join: `test.hits JOIN (small1 JOIN small2)`. The right side itself is a JOIN
-- subtree (with its own runtime filter build), so the walk through `findReadingStep` still
-- terminates at `test.hits` on the left.
SELECT count() FROM test.hits AS t1 INNER JOIN (SELECT s1.UserID FROM autopr_join_right_small AS s1 INNER JOIN autopr_join_right_small_2 AS s2 USING (UserID)) AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_8';

DROP TABLE autopr_join_right_small;
DROP TABLE autopr_join_right_small_2;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Fail if the estimated input bytes deviate from `ReadCompressedBytes` by more than a factor of 2,
-- or if no statistics were collected at all.
SELECT format('{} {} {}', log_comment, compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] statistics_input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE '04103_query_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE statistics_input_bytes = 0
   OR greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;
