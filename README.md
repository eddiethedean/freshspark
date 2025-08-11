# freshspark

Create **truly fresh** local Spark sessions with isolated temp dirs and reliable teardown.

- Isolates `spark.sql.warehouse.dir` and (optionally) embedded Derby **metastore** in unique temp dirs
- Defaults to **in-memory catalog** (no Derby) to avoid classic local locks
- Randomizes Spark UI port to avoid collisions and prints the UI URL
- Aggressively shuts down Py4J so the JVM actually exits
- Simple API: context manager, `(spark, cleanup)` pair, decorator, and a tiny CLI

## Install
```bash
pip install freshspark
```

## Quick start
```python
from freshspark import fresh_local_spark, get_fresh_local_spark

# Context manager (fresh per `with`)
with fresh_local_spark(app_name="etl", preset="dev") as spark:
    spark.range(10).show()

# Manual lifecycle
spark, cleanup = get_fresh_local_spark(app_name="demo", preset="fat")
try:
    spark.range(1000).summary().show()
finally:
    cleanup()
```

## Friendly features

- **Presets**: `preset="tiny" | "dev" | "fat"` set sane memory defaults.
- **No Hive by default**: in-memory catalog avoids Derby locks. Enable with `hive_metastore=True` if you need it.
- **Clean UI**: UI port auto-randomized; prints the URL once up.
- **Optional reuse (same process)**: `reuse_within_process=True` to keep one isolated session for repeated calls.
- **Decorator**: run any function inside a fresh session:

```python
from freshspark import ensure_fresh

@ensure_fresh(preset="dev")
def job(input_path: str, *, spark):
    return spark.read.csv(input_path, header=True).count()

print(job("data.csv"))
```

## CLI

```bash
# Open a REPL with `spark` ready:
freshspark repl --preset fat

# Stop any sticky active session in this process:
freshspark reset
```

## Jupyter tip

To be extra safe in notebooks:

```python
from freshspark import get_fresh_local_spark

spark, cleanup = get_fresh_local_spark(app_name="nb", preset="dev")
# ... work ...
# On finish (or in a finally cell):
cleanup()
```

## Why?
Local PySpark sessions can "stick"—leaving JVMs, metastore locks, or port clashes behind.
**freshspark** guarantees a clean slate every run.

## License
Apache 2.0
