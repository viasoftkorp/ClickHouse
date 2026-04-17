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

set enable_filesystem_cache=1;

SET parallel_replicas_prefer_local_join=1;

-- Use a tiny auxiliary `MergeTree` table as the right side of the JOIN. The autopr statistics
-- only cover the parallelized (left) side, whereas `ReadCompressedBytes` in `system.query_log`
-- aggregates reads from every source step, so any non-trivial right side would skew the ratio
-- even when the estimate itself is correct. A minimal right side keeps the two quantities
-- directly comparable.
DROP TABLE IF EXISTS autopr_join_right_small;
CREATE TABLE autopr_join_right_small (UserID UInt64) ENGINE = MergeTree ORDER BY UserID AS SELECT number FROM numbers(1000);

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

-- `INNER JOIN` with the small table on the SQL-left side. The DP join-order optimizer swaps the
-- sides so that `test.hits` ends up as the left (probe) child of the physical `JoinStep` while the
-- parallel-replicas plan keeps the original order (its DP limit is forced to `0` by the presence
-- of `allow_experimental_parallel_reading_from_replicas`). Commutative hashing at the `JoinStep`
-- level together with the transparency of row-preserving transforms (incl. runtime-filter
-- `FilterStep` / `BuildRuntimeFilter` / `Expression`) keeps the top-of-replicas hash stable across
-- the swap so autopr's node matching succeeds.
SELECT count() FROM autopr_join_right_small AS t1 INNER JOIN test.hits AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_6';

DROP TABLE autopr_join_right_small;

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
