from __future__ import annotations

import atexit
import gc
import os
import re
import shutil
import subprocess
import tempfile
import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

from pyspark.sql import SparkSession

"""
Core utilities for creating *fresh* local Spark sessions that don't clash with
previous runs, and that shut down cleanly every time.

Key features:
- Isolated temp dirs per session (warehouse + optional Derby metastore)
- In-memory catalog by default (no Derby locks)
- Randomized UI port to avoid collisions
- Optional reuse within the same Python process
- Aggressive JVM/Py4J shutdown during cleanup
- Java/PySpark compatibility check (fail-fast on unsupported JDKs)
"""

# --------------------------------------------------------------------------------------
# Environment compatibility helpers
# --------------------------------------------------------------------------------------

def _detect_java_major() -> Optional[int]:
    """
    Return Java major version (e.g., 8/11/17/21) or None if Java not found.

    Handles both "1.8.0_x" and "17.0.y" forms printed by `java -version`.
    """
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True)
    except FileNotFoundError:
        return None
    s = (proc.stderr or proc.stdout or "")
    # Examples:
    #   openjdk version "1.8.0_402"
    #   openjdk version "17.0.11"
    #   java version "21.0.3"
    m = re.search(r'version\s+"(?P<maj>\d+)(?:\.(?P<min>\d+))?', s)
    if not m:
        return None
    maj = int(m.group("maj"))
    if maj == 1:
        # Old style like 1.8 => Java 8
        minv = int(m.group("min") or 8)
        return 8 if minv == 8 else minv
    return maj


def _detect_pyspark_major() -> int:
    import pyspark  # local import to avoid hard dependency at import time
    return int(pyspark.__version__.split(".", 1)[0])


def _supported_java_for_pyspark(pyspark_major: int) -> set:
    """
    Spark 3.x supports Java 8/11/17; Spark 4.x supports Java 17/21.
    """
    return {17, 21} if pyspark_major >= 4 else {8, 11, 17}


def _check_java_support(soft: bool = False) -> Tuple[bool, str]:
    """
    Verify current Java is supported by installed PySpark/Spark.
    If soft=True, only warn and return (ok, msg). If soft=False, raise on unsupported.
    Honor FRESHSPARK_SKIP_JAVA_CHECK=1 to only warn.
    """
    py_major = _detect_pyspark_major()
    ok_set = _supported_java_for_pyspark(py_major)
    j = _detect_java_major()
    if j is None:
        msg = ("Java not found on PATH; Spark may fail to launch. "
               "Install a supported JDK (Spark 3.x: 8/11/17; Spark 4.x: 17/21).")
        if soft:
            warnings.warn(f"[freshspark] {msg}")
            return True, msg
        return True, msg  # don't block; Spark will error more specifically later
    if j not in ok_set:
        msg = (f"Detected Java {j}, but PySpark/Spark {py_major}.x supports {sorted(ok_set)}. "
               "Please install/switch JAVA_HOME to a supported JDK.")
        if soft or os.getenv("FRESHSPARK_SKIP_JAVA_CHECK", "").lower() in {"1", "true", "yes"}:
            warnings.warn(f"[freshspark] {msg}")
            return False, msg
        raise RuntimeError(msg)
    return True, f"Java {j} is supported for Spark {py_major}.x"


# --------------------------------------------------------------------------------------
# Config, presets, and module-level caches
# --------------------------------------------------------------------------------------

# In-process cache so we can optionally reuse a fresh session within the same Python process.
_ACTIVE: Dict[str, SparkSession] = {}
_ACTIVE_CLEANUP: Dict[str, Callable[[], None]] = {}

# Simple presets for user-friendly memory sizing & stability
_PRESETS = {
    # small notebooks, tiny ETL
    "tiny": {
        "spark.driver.memory": "1g",
        "spark.driver.maxResultSize": "512m",
    },
    # default dev
    "dev": {
        "spark.driver.memory": "2g",
        "spark.driver.maxResultSize": "1g",
    },
    # heavier local runs
    "fat": {
        "spark.driver.memory": "4g",
        "spark.driver.maxResultSize": "2g",
    },
}


@dataclass(frozen=True)
class FreshConfig:
    app_name: str = "freshspark"
    master: str = "local[*]"
    enable_ui: bool = True
    preset: str = "dev"               # one of: tiny, dev, fat
    reuse_within_process: bool = False
    print_ui_url: bool = True         # print the UI URL once the session is up
    hive_metastore: bool = False      # False => fully in-memory catalog; no Derby at all
    extra_confs: Optional[Dict[str, str]] = None


# --------------------------------------------------------------------------------------
# Internals
# --------------------------------------------------------------------------------------

def _make_isolated_dirs(prefix: str = "spark_local_") -> Tuple[str, str, str]:
    """
    Create isolated temporary directories for this run:
    - run_tmp: root temp folder for all artifacts
    - warehouse: spark.sql.warehouse.dir
    - metastore: embedded Derby location (kept out of CWD to avoid locks)
    """
    run_tmp = tempfile.mkdtemp(prefix=prefix)
    warehouse = os.path.join(run_tmp, "warehouse")
    metastore = os.path.join(run_tmp, "metastore")
    os.makedirs(warehouse, exist_ok=True)
    os.makedirs(metastore, exist_ok=True)
    return run_tmp, warehouse, metastore


def _shutdown_gateway(spark: SparkSession) -> None:
    """
    Best-effort shutdown for the Py4J callback server + gateway so the JVM exits.
    """
    try:
        sc = spark.sparkContext
    except Exception:
        return
    gw = getattr(sc, "_gateway", None)
    if gw:
        try:
            gw.shutdown_callback_server()
        except Exception:
            pass
        try:
            gw.close()
        except Exception:
            pass


def reset_active_session() -> None:
    """
    Stop any active SparkSession if present. Safe to call even if none exists.
    Also tries to shut down the Py4J gateway so the JVM exits.
    """
    prev = SparkSession.getActiveSession()
    if prev is None:
        return
    try:
        prev.stop()
    except Exception:
        pass
    _shutdown_gateway(prev)
    del prev
    gc.collect()


def _builder_from_config(cfg: FreshConfig, warehouse: str, metastore: str) -> SparkSession.Builder:
    """
    Build a SparkSession.Builder from our higher-level config.
    """
    preset = _PRESETS.get(cfg.preset, {})
    b = (
        SparkSession.builder
        .appName(f"{cfg.app_name}_{os.getpid()}_{int(time.time() * 1000)}")
        .master(cfg.master)
        # 0 = pick a free port; keep UI optional
        .config("spark.ui.port", "0" if cfg.enable_ui else "0")
        .config("spark.ui.enabled", "true" if cfg.enable_ui else "false")
        # Keep state isolated per run / encourage cleanup in local mode
        .config("spark.cleaner.referenceTracking", "true")
        .config("spark.cleaner.periodicGC.interval", "2min")
        # Avoid multiple SparkContexts in the same JVM
        .config("spark.driver.allowMultipleContexts", "false")
    )

    # Catalog / metastore behavior
    if cfg.hive_metastore:
        # Isolated Derby in a temp dir (no locks in CWD)
        b = (
            b.config("spark.sql.warehouse.dir", warehouse)
             .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore}")
        )
    else:
        # Fully in-memory catalog; avoids Derby entirely
        b = (
            b.config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.sql.warehouse.dir", warehouse)
        )

    # Apply preset + extras (allow user to override anything)
    for k, v in preset.items():
        b = b.config(k, v)
    if cfg.extra_confs:
        for k, v in cfg.extra_confs.items():
            b = b.config(k, v)

    return b


def _make_cleanup(run_tmp: str, app_name: str, spark_ref: SparkSession) -> Callable[[], None]:
    """
    Create a cleanup function that:
    - Stops Spark
    - Shuts down the JVM gateway
    - Clears reuse caches for this app_name
    - Removes temp directories
    """
    def _cleanup() -> None:
        try:
            spark_ref.stop()
        except Exception:
            pass
        _shutdown_gateway(spark_ref)
        # Drop cache entries so future reuse builds a new session
        _ACTIVE.pop(app_name, None)
        _ACTIVE_CLEANUP.pop(app_name, None)
        # Encourage GC and remove temp dirs
        gc.collect()
        shutil.rmtree(run_tmp, ignore_errors=True)
    return _cleanup


def _build_fresh_session(cfg: FreshConfig) -> Tuple[SparkSession, Callable[[], None]]:
    """
    Construct a SparkSession according to cfg, ensuring freshness and isolation.
    """
    # Fast fail / warn on unsupported Java
    _check_java_support(soft=False)

    # If reuse is requested and we have a cached one, return it
    if cfg.reuse_within_process and cfg.app_name in _ACTIVE:
        return _ACTIVE[cfg.app_name], _ACTIVE_CLEANUP[cfg.app_name]

    # Otherwise, stop any active session to avoid multiple contexts
    reset_active_session()

    # Build a new isolated session
    run_tmp, warehouse, metastore = _make_isolated_dirs(prefix=f"{cfg.app_name}_")
    builder = _builder_from_config(cfg, warehouse, metastore)
    spark = builder.getOrCreate()

    # Build cleanup and register as atexit fallback
    cleanup = _make_cleanup(run_tmp, cfg.app_name, spark)
    atexit.register(cleanup)

    # Optionally print the UI URL
    if cfg.print_ui_url and cfg.enable_ui:
        try:
            ui = spark.sparkContext.uiWebUrl
            if ui:
                print(f"[freshspark] Spark UI: {ui}")
        except Exception:
            pass

    # Cache for reuse if requested
    if cfg.reuse_within_process:
        _ACTIVE[cfg.app_name] = spark
        _ACTIVE_CLEANUP[cfg.app_name] = cleanup

    return spark, cleanup


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------

def get_fresh_local_spark(
    app_name: str = "freshspark",
    *,
    preset: str = "dev",
    reuse_within_process: bool = False,
    print_ui_url: bool = True,
    hive_metastore: bool = False,
    enable_ui: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
) -> Tuple[SparkSession, Callable[[], None]]:
    """
    Create a fresh local SparkSession and return (spark, cleanup_fn).

    Parameters
    ----------
    app_name : str
        Logical name for this session. Also used as cache key if reuse is enabled.
    preset : {"tiny","dev","fat"}
        Memory convenience presets. Defaults to "dev".
    reuse_within_process : bool
        If True, subsequent calls in this process with the same app_name will
        return the same (isolated) session and cleanup function.
    print_ui_url : bool
        If True, prints the Spark UI URL after session creation (when UI is enabled).
    hive_metastore : bool
        If False (default), use in-memory catalog to avoid Derby entirely.
        If True, enable embedded Derby metastore isolated under a temp dir.
    enable_ui : bool
        Enable the Spark web UI (on a random free port).
    extra_confs : dict
        Additional Spark configs to apply (can override presets/defaults).

    Returns
    -------
    (SparkSession, Callable[[], None])
        The session and a cleanup function that MUST be called when done if you
        are not using the context manager API.
    """
    cfg = FreshConfig(
        app_name=app_name,
        preset=preset,
        reuse_within_process=reuse_within_process,
        print_ui_url=print_ui_url,
        hive_metastore=hive_metastore,
        enable_ui=enable_ui,
        extra_confs=extra_confs,
    )
    return _build_fresh_session(cfg)


@contextmanager
def fresh_local_spark(
    app_name: str = "freshspark",
    *,
    preset: str = "dev",
    print_ui_url: bool = True,
    hive_metastore: bool = False,
    enable_ui: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
):
    """
    Context manager that yields a brand-new local SparkSession and guarantees cleanup.
    Always fresh per `with` block (no reuse).
    """
    spark, cleanup = get_fresh_local_spark(
        app_name=app_name,
        preset=preset,
        reuse_within_process=False,
        print_ui_url=print_ui_url,
        hive_metastore=hive_metastore,
        enable_ui=enable_ui,
        extra_confs=extra_confs,
    )
    try:
        yield spark
    finally:
        cleanup()


def ensure_fresh(
    *,
    app_name: str = "freshspark",
    preset: str = "dev",
    print_ui_url: bool = True,
    hive_metastore: bool = False,
    enable_ui: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
):
    """
    Decorator to run a function with a guaranteed fresh local Spark session.
    The function must accept a `spark` kwarg (will be injected).

    Example
    -------
    @ensure_fresh(preset="dev")
    def job(path: str, *, spark):
        return spark.read.csv(path, header=True).count()
    """
    def _wrap(fn):
        def _inner(*args, **kwargs):
            with fresh_local_spark(
                app_name=app_name,
                preset=preset,
                print_ui_url=print_ui_url,
                hive_metastore=hive_metastore,
                enable_ui=enable_ui,
                extra_confs=extra_confs,
            ) as spark:
                kwargs["spark"] = spark
                return fn(*args, **kwargs)
        return _inner
    return _wrap
