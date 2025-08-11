import os
import time
import pytest
from pyspark.sql import SparkSession

from freshspark import (
    fresh_local_spark,
    get_fresh_local_spark,
    reset_active_session,
    ensure_fresh,
)

def _warehouse_dir(spark):
    # Spark stores the effective value in SparkConf; try both
    try:
        return spark.conf.get("spark.sql.warehouse.dir")
    except Exception:
        return spark.sparkContext.getConf().get("spark.sql.warehouse.dir")

def _driver_extra_javaopts(spark):
    return spark.sparkContext.getConf().get("spark.driver.extraJavaOptions", None)


def test_context_manager_creates_fresh_and_isolated():
    with fresh_local_spark(app_name="t1", preset="tiny") as s1:
        assert s1.range(3).count() == 3
        w1 = _warehouse_dir(s1)
        assert os.path.isabs(w1)

    # New session gets a different isolated warehouse
    with fresh_local_spark(app_name="t1", preset="tiny") as s2:
        assert s2.range(2).count() == 2
        w2 = _warehouse_dir(s2)
        assert w2 != w1


def test_manual_lifecycle_and_cleanup():
    spark, cleanup = get_fresh_local_spark(app_name="manual", preset="tiny")
    try:
        assert spark.range(1).count() == 1
        # UI config should be randomized port 0 (auto-pick)
        assert spark.conf.get("spark.ui.port") == "0"
    finally:
        cleanup()
    # After cleanup, no active session should remain
    assert spark.sparkContext._jsc is None or spark.sparkContext._gateway is None
    assert reset_active_session() is None  # idempotent no-op


@pytest.mark.parametrize("preset,mem,maxres", [
    ("tiny", "1g", "512m"),
    ("dev",  "2g", "1g"),
    ("fat",  "4g", "2g"),
])
def test_presets_apply_memory_defaults(preset, mem, maxres):
    with fresh_local_spark(app_name=f"mem_{preset}", preset=preset) as spark:
        conf = spark.sparkContext.getConf()
        assert conf.get("spark.driver.memory") == mem
        assert conf.get("spark.driver.maxResultSize") == maxres


def test_extra_confs_are_applied():
    with fresh_local_spark(
        app_name="extras",
        extra_confs={"spark.sql.shuffle.partitions": "5"},
        preset="tiny",
    ) as spark:
        assert spark.conf.get("spark.sql.shuffle.partitions") == "5"


def test_default_in_memory_catalog_no_derby():
    with fresh_local_spark(app_name="nometa", hive_metastore=False, preset="tiny") as spark:
        # In-memory catalog selected
        assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
        # No Derby option set on driver when hive_metastore=False
        assert _driver_extra_javaopts(spark) in (None, "")


def test_hive_metastore_true_sets_derby_location():
    with fresh_local_spark(app_name="withmeta", hive_metastore=True, preset="tiny") as spark:
        # Warehouse directory exists and is isolated
        w = _warehouse_dir(spark)
        assert os.path.isabs(w)
        # Derby home is set via driver extraJavaOptions
        javaopts = _driver_extra_javaopts(spark)
        assert javaopts and "derby.system.home=" in javaopts


def test_reset_active_session_is_idempotent():
    # No session yet
    reset_active_session()
    # Create then reset twice
    s, c = get_fresh_local_spark(app_name="reset", preset="tiny")
    try:
        assert s.range(2).count() == 2
    finally:
        c()
    reset_active_session()
    reset_active_session()  # should not raise


def test_rapid_create_stop_sequences_avoid_conflicts():
    # Stress a little to catch port/warehouse conflicts
    last_warehouse = None
    for i in range(4):
        with fresh_local_spark(app_name=f"loop{i}", preset="tiny") as spark:
            assert spark.range(5).count() == 5
            w = _warehouse_dir(spark)
            if last_warehouse is not None:
                assert w != last_warehouse
            last_warehouse = w
        time.sleep(0.05)


def test_decorator_runs_and_cleans_up():
    seen_app_ids = []

    @ensure_fresh(app_name="decor", preset="tiny")
    def job(n: int, *, spark):
        seen_app_ids.append(spark.sparkContext.applicationId)
        return spark.range(n).count()

    assert job(7) == 7
    # After decorator returns, there should be no active session
    assert SparkSession.getActiveSession() is None  # type: ignore[name-defined]


def test_reuse_within_process_returns_same_instance():
    s1, cleanup1 = get_fresh_local_spark(app_name="reuse", preset="tiny", reuse_within_process=True)
    try:
        s2, cleanup2 = get_fresh_local_spark(app_name="reuse", preset="tiny", reuse_within_process=True)
        assert s1 is s2
        assert s1.range(1).count() == 1
    finally:
        # cleanup both (same underlying cleanup) to be safe
        cleanup2()
        cleanup1()


@pytest.mark.xfail(reason="Cache is not cleared on cleanup; consider clearing _ACTIVE on cleanup.")
def test_reuse_after_cleanup_should_build_new_session():
    s1, cleanup = get_fresh_local_spark(app_name="reuse2", preset="tiny", reuse_within_process=True)
    app_id_1 = s1.sparkContext.applicationId
    cleanup()
    # With reuse enabled and same app_name, we expect a *new* session after cleanup
    s2, cleanup2 = get_fresh_local_spark(app_name="reuse2", preset="tiny", reuse_within_process=True)
    app_id_2 = s2.sparkContext.applicationId
    try:
        assert app_id_2 != app_id_1
    finally:
        cleanup2()
