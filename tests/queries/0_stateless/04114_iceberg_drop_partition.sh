#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y'), (2, 'z'), (3, 'w')"

echo "--- before drop ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 2"

echo "--- after drop partition 2 ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

# Drop a non-existing partition: should be a no-op
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 99"

echo "--- after drop partition 99 (no-op) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

# Drop the last remaining partitions
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 3"

echo "--- after dropping all partitions ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# DROP PARTITION ALL should be rejected
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION ALL" 2>&1 | grep -o 'NOT_IMPLEMENTED' | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
