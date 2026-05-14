# freshspark

[![CI](https://github.com/eddiethedean/freshspark/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/freshspark/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/freshspark.svg)](https://pypi.org/project/freshspark/)
[![Python versions](https://img.shields.io/pypi/pyversions/freshspark.svg)](https://pypi.org/project/freshspark/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Small helpers for **local PySpark** that start each run in a clean sandbox and tear sessions down reliably: isolated warehouse temp dirs, optional embedded Derby kept out of the working tree, in-memory catalog by default, randomized Spark UI port, and aggressive Py4J / JVM shutdown so the process can exit normally.

**Use it when** notebooks or scripts leave metastore locks, `derby.log` in the wrong place, Spark UI port collisions, or JVMs that refuse to die after `SparkSession.stop()`.

---

## Requirements

| | |
| --- | --- |
| **Python** | 3.9 or newer |
| **JDK** | On `PATH` (or via `JAVA_HOME`). Spark 3.x line: Java 8, 11, or 17; **PySpark 3.5+** is also validated against **Java 21**. |
| **PySpark** | Declared dependency is **PySpark 3.5.x** (`pyspark>=3.5,<4`) for predictable local startup. Spark 4 is not pinned here; if you override to PySpark 4.x, use Java 17 or 21 and expect to manage compatibility yourself. |

---

## Install

```bash
pip install freshspark
```

**Development** (editable install, tests, Ruff):

```bash
pip install -e ".[dev]"
ruff format --check freshspark tests
ruff check freshspark tests
mypy freshspark tests
pytest
```

---

## Quick start

```python
from freshspark import fresh_local_spark, get_fresh_local_spark

# Context manager: new session every `with` block, cleanup on exit
with fresh_local_spark(app_name="etl", preset="dev") as spark:
    spark.range(10).show()

# Manual lifecycle: always call cleanup() when finished (or use try/finally)
spark, cleanup = get_fresh_local_spark(app_name="demo", preset="fat")
try:
    spark.range(1000).summary().show()
finally:
    cleanup()
```

---

## Public API

| Symbol | Role |
| --- | --- |
| `fresh_local_spark(...)` | Context manager yielding a **new** `SparkSession` per `with` block (no reuse). |
| `get_fresh_local_spark(...)` | Returns `(spark, cleanup)`. You **must** call `cleanup()` when done unless you use the context manager. |
| `reset_active_session()` | Stops the active session, closes the gateway, and **clears in-process reuse cache** entries that pointed at that session (or are already dead). Safe to call repeatedly. |
| `ensure_fresh(...)` | Decorator that runs the wrapped function inside `fresh_local_spark`; injects `spark` as a keyword argument. Do not pass `spark=` yourself (a `TypeError` is raised if you do). |

---

## Configuration highlights

### Presets

`preset` is one of `tiny`, `dev`, or `fat`. They set driver memory and `maxResultSize` to sensible defaults. Any other string logs a **warning** and applies no preset keys (you can still set everything via `extra_confs`).

| Preset | `spark.driver.memory` | `spark.driver.maxResultSize` |
| --- | --- | --- |
| `tiny` | `1g` | `512m` |
| `dev` | `2g` | `1g` |
| `fat` | `4g` | `2g` |

### Catalog and warehouse

- **Default** (`hive_metastore=False`): `spark.sql.catalogImplementation=in-memory` and an isolated `spark.sql.warehouse.dir` under a temp root—no embedded Derby in the default path, so you avoid the usual Derby lock files in the project directory.
- **Hive-style metastore** (`hive_metastore=True`): warehouse and Derby home (`-Dderby.system.home=...`) both live under the same isolated temp tree.

If you pass `extra_confs` with `spark.driver.extraJavaOptions` while `hive_metastore=True`, that value is **merged** after the required Derby system home flag so your JVM flags do not accidentally wipe metastore configuration.

### Other knobs

- **`enable_ui` / `print_ui_url`**: Spark UI on a free port (`spark.ui.port=0`); optionally print the URL once the session is up.
- **`extra_confs`**: flat `dict[str, str]` merged last so you can override presets or Spark defaults.
- **`reuse_within_process=True`**: same Python process + same `app_name` returns the same `(spark, cleanup)` until `cleanup()` or `reset_active_session()` runs; dead cached sessions are replaced automatically on the next request.

---

## CLI

```bash
# Python REPL with `spark` already constructed
freshspark repl --preset fat

# Stop the active SparkSession in this interpreter (also reconciles reuse cache)
freshspark reset
```

| Command | Common flags |
| --- | --- |
| `freshspark repl` | `--app-name`, `--preset tiny|dev|fat`, `--hive`, `--no-ui` |
| `freshspark reset` | _(none)_ |

---

## Jupyter and long-running kernels

Prefer an explicit cleanup cell so temp dirs and the JVM are released even if the kernel stays alive:

```python
from freshspark import get_fresh_local_spark

spark, cleanup = get_fresh_local_spark(app_name="nb", preset="dev")
# ... work ...
cleanup()
```

If another library left a sticky session in this kernel, call `reset_active_session()` here. The `freshspark reset` CLI only affects the interpreter where that command runs (for example a terminal REPL), not a separate Jupyter kernel.

---

## Environment variables

| Variable | Effect |
| --- | --- |
| `FRESHSPARK_SKIP_JAVA_CHECK` | If set to `1`, `true`, or `yes`, an unsupported Java / Spark pairing **warns** instead of raising during session construction. |

---

## Why this exists

Local PySpark is great until it is not: JVMs that linger, Derby files under `cwd`, warehouse dirs shared across runs, and UI ports that collide. **freshspark** centralizes a small set of Spark configs and lifecycle rules so each run gets an isolated temp layout and a cleanup path that actually runs (including an `atexit` safety net, with idempotent cleanup so manual `cleanup()` plus process exit does not misbehave).

---

## Project links

- **Homepage / source:** [github.com/eddiethedean/freshspark](https://github.com/eddiethedean/freshspark)
- **Issues:** [github.com/eddiethedean/freshspark/issues](https://github.com/eddiethedean/freshspark/issues)

---

## License

Apache 2.0 (see [LICENSE](LICENSE)).
