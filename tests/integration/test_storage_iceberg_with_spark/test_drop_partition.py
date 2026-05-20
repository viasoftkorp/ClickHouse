import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    execute_spark_query_general,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_with_evolved_spec_is_rejected(started_cluster_iceberg_with_spark, storage_type):
    """`ALTER TABLE ... DROP PARTITION` must refuse to operate on Iceberg
    tables whose partition spec has evolved (more than one entry in
    `partition-specs`). Manifests written under the old spec encode the
    partition tuple under a different transform set, and silently rewriting
    them against the new spec would produce a corrupt manifest. The executor
    rejects the operation with `NOT_IMPLEMENTED` until per-manifest spec
    resolution lands."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = f"test_drop_partition_evolved_spec_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, table_name, query)

    # Original table with single partition spec on `tag`.
    spark_query(
        f"""
            CREATE TABLE {table_name} (tag INT, k STRING, v INT)
            USING iceberg
            PARTITIONED BY (identity(tag))
            OPTIONS('format-version'='2')
        """)
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'a', 10), (2, 'b', 20), (1, 'c', 30)")

    # Evolve the partition spec by adding a second field.
    spark_query(f"ALTER TABLE {table_name} ADD PARTITION FIELD identity(k)")
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'd', 40), (3, 'e', 50)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    create_iceberg_table(storage_type, instance, table_name, started_cluster_iceberg_with_spark)

    # Sanity check the read side still works.
    assert instance.query(f"SELECT count() FROM {table_name}") == "5\n"

    # DROP PARTITION must refuse because the table now carries two partition specs.
    error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert "evolved partition specs" in error, error

    # And data is untouched.
    assert instance.query(f"SELECT count() FROM {table_name}") == "5\n"
