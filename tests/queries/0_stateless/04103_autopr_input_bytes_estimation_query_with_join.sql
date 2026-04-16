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

-- `query_plan_join_swap_table='auto'` can make `considerEnablingParallelReplicas` and the
-- `query_plan_with_parallel_replicas_builder` reach different swap decisions for the same query,
-- which in turn causes their `PrewhereInfo.prewhere_actions` `ActionsDAG`s to hash differently
-- even though the resulting plans are logically identical. Pin the orientation until that is fixed.
SET query_plan_join_swap_table='false';

-- `INNER JOIN` of two MergeTree tables, with the right side much smaller than the left.
-- With `parallel_replicas_prefer_local_join=1`, the left side is the parallelized side,
-- so the reported `RuntimeDataflowStatisticsInputBytes` should approximate the bytes read from `test.hits`.
SELECT count() FROM test.hits AS t1 INNER JOIN test.visits AS t2 USING (UserID) FORMAT Null SETTINGS log_comment='04103_query_1';

-- Same JOIN with a filter on the left side. `URL` is not part of the primary key, so this filter
-- does not prune granules via PK index analysis; the full left side is still read from disk and
-- therefore `ReadCompressedBytes` stays comparable to the collected statistics. Using a PK column
-- like `CounterID` here would slash the number of scanned granules and make the two quantities
-- incomparable via the simple ratio check below, since they would measure different things.
SELECT count() FROM test.hits AS t1 INNER JOIN test.visits AS t2 USING (UserID) WHERE t1.URL LIKE '%com%' FORMAT Null SETTINGS log_comment='04103_query_2';

-- `LEFT JOIN` with aggregation on top.
SELECT t1.CounterID, count() AS c FROM test.hits AS t1 LEFT JOIN test.visits AS t2 USING (UserID) GROUP BY t1.CounterID ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='04103_query_3', max_block_size=65409;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Fail if the estimated input bytes deviate from `ReadCompressedBytes` by more than a factor of 2,
-- or if no statistics were collected at all (i.e. `RuntimeDataflowStatisticsInputBytes` is missing / zero).
-- For JOINs, `ReadCompressedBytes` aggregates both sides while the statistics cover only the
-- parallelized (left) side, so queries are chosen such that the right side is small enough
-- to keep the ratio within the threshold.
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
