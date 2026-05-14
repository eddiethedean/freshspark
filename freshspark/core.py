"""Core utilities for fresh local Spark sessions with isolated temp dirs and teardown.

Isolated warehouse (and optional Derby metastore), in-memory catalog by default,
randomized UI port, optional in-process reuse, Py4J shutdown, and Java compatibility checks.
"""

from __future__ import annotations

import atexit
import functools
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
from typing import Callable

from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Environment compatibility helpers
# --------------------------------------------------------------------------------------


def _detect_java_major() -> int | None:
    """
    Return Java major version (e.g., 8/11/17/21) or None if Java not found.

    Handles both "1.8.0_x" and "17.0.y" forms printed by `java -version`.
    Note: `java -version` often exits with code 1 while still printing valid output;
    we parse stderr/stdout and ignore returncode when text is present.
    """
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True)
    except FileNotFoundError:
        return None
    s = (proc.stderr or proc.stdout or "").strip()
    if not s:
        return None
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


def _detect_pyspark_major_minor() -> tuple[int, int]:
    import pyspark  # local import to avoid hard dependency at import time

    parts = pyspark.__version__.split(".", 2)
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    return major, minor


def _supported_java_for_pyspark(pyspark_major: int, pyspark_minor: int) -> set[int]:
    """
    Spark 4.x supports Java 17/21.
    Spark 3.5+ commonly runs on Java 21 as well as 8/11/17; older 3.x lines follow upstream.
    """
    if pyspark_major >= 4:
        return {17, 21}
    if pyspark_major == 3 and pyspark_minor >= 5:
        return {8, 11, 17, 21}
    return {8, 11, 17}


def _check_java_support(soft: bool = False) -> tuple[bool, str]:
    """
    Verify current Java is supported by installed PySpark/Spark.
    If soft=True, only warn and return (ok, msg). If soft=False, raise on unsupported.
    Honor FRESHSPARK_SKIP_JAVA_CHECK=1 to only warn.
    """
    py_major, py_minor = _detect_pyspark_major_minor()
    ok_set = _supported_java_for_pyspark(py_major, py_minor)
    j = _detect_java_major()
    if j is None:
        msg = ("Java not found on PATH; Spark may fail to launch. "
               "Install a supported JDK (Spark 3.x: 8/11/17; Spark 3.5+: also 21; Spark 4.x: 17/21).")
        if soft:
            warnings.warn(f"[freshspark] {msg}", stacklevel=2)
            return True, msg
        return True, msg  # don't block; Spark will error more specifically later
    if j not in ok_set:
        msg = (f"Detected Java {j}, but PySpark/Spark {py_major}.x supports {sorted(ok_set)}. "
               "Please install/switch JAVA_HOME to a supported JDK.")
        if soft or os.getenv("FRESHSPARK_SKIP_JAVA_CHECK", "").lower() in {"1", "true", "yes"}:
            warnings.warn(f"[freshspark] {msg}", stacklevel=2)
            return False, msg
        raise RuntimeError(msg)
    return True, f"Java {j} is supported for Spark {py_major}.x"


# --------------------------------------------------------------------------------------
# Config, presets, and module-level caches
# --------------------------------------------------------------------------------------

# In-process cache so we can optionally reuse a fresh session within the same Python process.
_ACTIVE: dict[str, SparkSession] = {}
_ACTIVE_CLEANUP: dict[str, Callable[[], None]] = {}

# Simple presets for user-friendly memory sizing & stability
_PRESETS: dict[str, dict[str, str]] = {
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
    extra_confs: dict[str, str] | None = None


# --------------------------------------------------------------------------------------
# Internals
# --------------------------------------------------------------------------------------


def _make_isolated_dirs(prefix: str = "spark_local_") -> tuple[str, str, str]:
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


def _reuse_session_is_dead(spark: SparkSession) -> bool:
    """True if the session's SparkContext appears stopped (stale cache entry)."""
    try:
        sc = spark.sparkContext
        jsc = getattr(sc, "_jsc", None)
        return jsc is None
    except Exception:
        return True


def _run_reuse_cleanup(app_name: str) -> None:
    """Pop and invoke cleanup for a reuse cache key if present."""
    c = _ACTIVE_CLEANUP.pop(app_name, None)
    _ACTIVE.pop(app_name, None)
    if c is not None:
        c()


def _evict_reuse_entries_for_session(session: SparkSession | None) -> None:
    """Remove reuse cache entries pointing at this session object and run their cleanups."""
    if session is None:
        return
    for key in [k for k, s in _ACTIVE.items() if s is session]:
        _run_reuse_cleanup(key)


def _evict_dead_reuse_entries() -> None:
    """Remove reuse cache entries whose SparkSession is already stopped."""
    for key in list(_ACTIVE.keys()):
        s = _ACTIVE.get(key)
        if s is not None and _reuse_session_is_dead(s):
            _run_reuse_cleanup(key)


def reset_active_session() -> None:
    """
    Stop any active SparkSession if present. Safe to call even if none exists.
    Also tries to shut down the Py4J gateway so the JVM exits.
    Drops stale reuse cache entries so a subsequent reuse_within_process call
    cannot return a stopped session.
    """
    prev = SparkSession.getActiveSession()
    if prev is not None:
        try:
            prev.stop()
        except Exception:
            pass
        _shutdown_gateway(prev)
        _evict_reuse_entries_for_session(prev)
    _evict_dead_reuse_entries()
    gc.collect()


def _builder_from_config(cfg: FreshConfig, warehouse: str, metastore: str) -> SparkSession.Builder:
    """
    Build a SparkSession.Builder from our higher-level config.
    """
    if cfg.preset not in _PRESETS:
        warnings.warn(
            f"[freshspark] Unknown preset {cfg.preset!r}; no preset memory settings applied. "
            f"Use one of: {', '.join(sorted(_PRESETS))}.",
            stacklevel=4,
        )
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

    derby_java_opt = f"-Dderby.system.home={metastore}"

    # Catalog / metastore behavior
    if cfg.hive_metastore:
        # Isolated Derby in a temp dir (no locks in CWD)
        b = (
            b.config("spark.sql.warehouse.dir", warehouse)
             .config("spark.driver.extraJavaOptions", derby_java_opt)
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
            if (
                cfg.hive_metastore
                and k == "spark.driver.extraJavaOptions"
                and derby_java_opt not in v
            ):
                merged = f"{derby_java_opt} {v}".strip()
                b = b.config(k, merged)
            else:
                b = b.config(k, v)

    return b


def _make_cleanup(run_tmp: str, app_name: str, spark_ref: SparkSession) -> Callable[[], None]:
    """
    Create an idempotent cleanup function that:
    - Stops Spark
    - Shuts down the JVM gateway
    - Clears reuse caches for this app_name
    - Removes temp directories
    """
    done: list[bool] = [False]

    def _cleanup() -> None:
        if done[0]:
            return
        done[0] = True
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


def _register_atexit(cleanup: Callable[[], None]) -> None:
    """Register cleanup with atexit; cleanup is idempotent so duplicate runs are safe."""

    def _guard() -> None:
        cleanup()

    atexit.register(_guard)


def _build_fresh_session(cfg: FreshConfig) -> tuple[SparkSession, Callable[[], None]]:
    """
    Construct a SparkSession according to cfg, ensuring freshness and isolation.
    """
    # Fast fail / warn on unsupported Java
    _check_java_support(soft=False)

    # If reuse is requested and we have a cached one, return it (if still alive)
    if cfg.reuse_within_process and cfg.app_name in _ACTIVE:
        cached = _ACTIVE[cfg.app_name]
        if not _reuse_session_is_dead(cached):
            return cached, _ACTIVE_CLEANUP[cfg.app_name]
        _run_reuse_cleanup(cfg.app_name)

    # Otherwise, stop any active session to avoid multiple contexts
    reset_active_session()

    # Build a new isolated session
    run_tmp, warehouse, metastore = _make_isolated_dirs(prefix=f"{cfg.app_name}_")
    builder = _builder_from_config(cfg, warehouse, metastore)
    try:
        spark = builder.getOrCreate()
    except Exception:
        shutil.rmtree(run_tmp, ignore_errors=True)
        raise

    # Build cleanup and register as atexit fallback
    cleanup = _make_cleanup(run_tmp, cfg.app_name, spark)
    _register_atexit(cleanup)

    # Optionally print the Spark UI URL
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
    extra_confs: dict[str, str] | None = None,
) -> tuple[SparkSession, Callable[[], None]]:
    """
    Create a fresh local SparkSession and return (spark, cleanup_fn).

    Parameters
    ----------
    app_name : str
        Logical name for this session. Also used as cache key if reuse is enabled.
    preset : {"tiny","dev","fat"}
        Memory convenience presets. Defaults to "dev". Unknown names log a warning
        and apply no preset memory keys.
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
        With ``hive_metastore=True``, a user ``spark.driver.extraJavaOptions`` value
        is merged after the required ``-Dderby.system.home=...`` flag so Derby is not dropped.

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
    extra_confs: dict[str, str] | None = None,
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
    extra_confs: dict[str, str] | None = None,
):
    """
    Decorator to run a function with a guaranteed fresh local Spark session.
    The wrapped function must accept a ``spark`` keyword argument (injected here).
    Passing ``spark=`` from the caller is not allowed.

    Example
    -------
    @ensure_fresh(preset="dev")
    def job(path: str, *, spark):
        return spark.read.csv(path, header=True).count()
    """
    def _wrap(fn):
        @functools.wraps(fn)
        def _inner(*args, **kwargs):
            if "spark" in kwargs:
                raise TypeError("ensure_fresh: pass no 'spark' kwarg; it is injected by the decorator.")
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
