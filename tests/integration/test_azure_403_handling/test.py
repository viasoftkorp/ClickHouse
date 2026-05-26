#!/usr/bin/env python3
"""Tests for Azure 403 / PATH_ACCESS_DENIED handling."""
import os

import pytest

from helpers.cluster import ClickHouseCluster


AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER = "cont"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Note that "azure_disk.xml" uses `from_env="AZURITE_STORAGE_ACCOUNT_URL"` so each xdist worker picks up its own azurite endpoint
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[os.path.join(SCRIPT_DIR, "configs", "azure_disk.xml")],
    with_azurite=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True, scope="function")
def reset_failpoint_between_tests(started_cluster):
    # even with try/finally per test, ensure no failpoint state leaks."""
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass
    yield
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass


def _create_table(endpoint, name, blob):
    node.query(
        f"""
        CREATE TABLE {name} (k UInt64, v String)
        ENGINE = AzureBlobStorage(
            '{endpoint}', '{CONTAINER}', '{blob}',
            '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV'
        )
        """
    )


def test_sanity_check(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t", "basic.csv")
    node.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    assert node.query("SELECT count() FROM t").strip() == "3"
    assert node.query("SELECT v FROM t WHERE k = 2").strip() == "b"

    files = node.query(
        f"""
        SELECT DISTINCT _file
        FROM azureBlobStorage(
            '{endpoint}', '{CONTAINER}', '*',
            '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV'
        )
        ORDER BY _file
        FORMAT TabSeparated
        """
    ).strip().splitlines()
    assert files == ["basic.csv"]


def test_injected_403_surfaces_as_path_access_denied(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t_403", "failpoint.csv")
    node.query("INSERT INTO t_403 VALUES (1, 'a'), (2, 'b')")

    assert node.query("SELECT count() FROM t_403").strip() == "2"

    # Caches would mask the failpoint by serving the next read from memory.
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")

    # All subsequent Azure HTTP requests synthesise a 403 until disabled.
    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("SELECT count() FROM t_403")
        assert "PATH_ACCESS_DENIED" in err, f"expected PATH_ACCESS_DENIED, got:\n{err}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")


def test_403_during_merge_surfaces_as_path_access_denied(started_cluster):
    # Real-life scenario - failure during a background merge while reading parts from Azure.
    # Asserts the resulting error is PATH_ACCESS_DENIED and NOT POTENTIALLY_BROKEN_DATA_PART.
    node.query(
        """
        CREATE TABLE t_merge (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_merge SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_merge").strip() == "300"

    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_merge FINAL")
        assert "PATH_ACCESS_DENIED" in err, f"expected PATH_ACCESS_DENIED, got:\n{err}"
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err, (
            f"unexpected broken-part error:\n{err}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
