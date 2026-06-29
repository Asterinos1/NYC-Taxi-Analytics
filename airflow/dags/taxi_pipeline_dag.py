"""
NYC Yellow Taxi Medallion Pipeline — Airflow DAG v2.2.0
========================================================
Demonstrates four Airflow capabilities beyond bare SparkSubmitOperator:

  1. FileSensor           — blocks ingest until the raw CSV is present on disk.
  2. TaskFlow @task       — pure-Python task (count_silver_partitions) using the
                            declarative TaskFlow API, contrasting with the
                            imperative SparkSubmitOperator style.
  3. XCom                — @task pushes the silver partition-file count; the
                            downstream BranchPythonOperator pulls it to decide
                            which branch to take.
  4. BranchPythonOperator — routes to full analytics when the silver layer has
                            enough data, or to an 'insufficient_data' short-
                            circuit path otherwise. Both paths join at
                            'pipeline_done' (trigger_rule=none_failed_min_one_success).

Pipeline topology (normal run):

    wait_for_raw_data (FileSensor)
        → ingest_bronze        (SparkSubmitOperator)
        → clean_silver         (SparkSubmitOperator)
        → count_silver_partitions (@task / XCom producer)
        → branch_on_volume     (BranchPythonOperator / XCom consumer)
            ┌─ analytics_gold  (SparkSubmitOperator)
            │     → validate_quality_gate (SparkSubmitOperator)
            │           ↘
            └─ insufficient_data (EmptyOperator)
                          ↘
                        pipeline_done (EmptyOperator, join)

Fault-injection mode (FAULT_INJECT=true):

    ... clean_silver
        → fault_inject_silver  (PythonOperator — writes 3 bad rows to silver)
        → count_silver_partitions
        → ... → validate_quality_gate   ← FAILS as expected (demo of gate)

Defense-in-depth rationale
--------------------------
CleanJob already filters passenger_count <= 0 and null PULocationID/DOLocationID
from bronze data, so those rows cannot reach silver under normal operation.
ValidateJob re-checks the same rules on silver NOT because the current-run data
is suspect, but to guard against *upstream regressions*: if CleanJob is
accidentally skipped, its filters weakened, or a new upstream source bypasses it
entirely, the gate will catch the corruption before it contaminates gold.
This is defense-in-depth, not redundancy.

To demonstrate the gate catching bad data:
  1. Set FAULT_INJECT=true in the Airflow environment (or Connections → Variables).
  2. Trigger the DAG — the fault_inject_silver task writes 3 synthetic bad rows
     directly into the partitioned silver/ directory ValidateJob reads.
  3. ValidateJob will throw RuntimeException and fail the DAG run.
  4. Set FAULT_INJECT=false (or remove it) and re-trigger for the passing run.
  Alternatively run scripts/inject_bad_silver.py / purge_injected_silver.py
  manually for a run-independent demo.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Paths as mounted inside the containers (shared Docker volume).
RAW_DATA_PATH = "/data/raw"
SILVER_PATH = "/data/silver"

# Minimum number of Parquet part-files expected in the silver layer.
# A newly cleaned dataset of ~12 M rows typically produces ≥ 36 files
# (12 year/month partitions × ≥ 3 parts each).  Set low so the branch
# exercises the "happy path" in normal runs; override via Airflow Variable
# when testing with tiny data.
MIN_PARTITION_FILES: int = int(os.environ.get("MIN_PARTITION_FILES", "1"))

# Fat JAR produced by `sbt assembly`, mounted at /spark-app inside containers.
JAR_PATH = "/spark-app/target/scala-2.12/nyc-taxi-analytics-assembly-2.0.0.jar"

# Fault-injection toggle.  Set FAULT_INJECT=true to enable the optional task
# that writes bad rows directly into silver, triggering a quality-gate failure.
# Remove or set to any other value to disable (normal pipeline behaviour).
try:
    from airflow.models import Variable
    FAULT_INJECT: bool = (
        os.environ.get("FAULT_INJECT", "false").lower() == "true" or
        Variable.get("FAULT_INJECT", "false").lower() == "true"
    )
except Exception:
    FAULT_INJECT: bool = os.environ.get("FAULT_INJECT", "false").lower() == "true"

# Injected artefact details (must match inject_bad_silver.py).
_INJECT_PARTITION = "year=2021/month=1"
_INJECT_FILENAME  = "INJECTED_BAD_ROWS.parquet"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

dag = DAG(
    "nyc_taxi_medallion_pipeline",
    default_args=default_args,
    description="NYC Yellow Taxi Medallion Pipeline v2.2.0",
    schedule=None,  # trigger manually or via backfill
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "spark", "medallion"],
)

# ---------------------------------------------------------------------------
# Shared Spark submit kwargs (DRY)
# ---------------------------------------------------------------------------

_SPARK_BASE = dict(
    application=JAR_PATH,
    conn_id="spark_default",
    driver_memory="1G",
    executor_memory="2G",
    executor_cores=2,
    conf={
        "spark.hadoop.fs.permissions.umask-mode": "000",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///data/spark-events",
    },
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task 0 — FileSensor
# Blocks the pipeline until the raw CSV file exists on the shared volume.
# Demonstrates scheduling / dependency on external state rather than
# always assuming data is present.
#
# Requires the 'fs_default' Airflow connection (conn_type = "File (path)",
# extra = {"path": "/"}).  The connection is pre-configured in
# docker/airflow/connections.json or via the Airflow UI.
# ---------------------------------------------------------------------------

wait_for_raw_data = FileSensor(
    task_id="wait_for_raw_data",
    filepath=RAW_DATA_PATH,
    fs_conn_id="fs_default",
    poke_interval=30,   # check every 30 s
    timeout=600,        # give up after 10 min
    mode="poke",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Tasks 1 & 2 — SparkSubmitOperator (ingest + clean, unchanged)
# ---------------------------------------------------------------------------

ingest_task = SparkSubmitOperator(
    task_id="ingest_bronze",
    java_class="jobs.IngestJob",
    **_SPARK_BASE,
)

clean_task = SparkSubmitOperator(
    task_id="clean_silver",
    java_class="jobs.CleanJob",
    env_vars={
        "TARGET_SAMPLE_SIZE": os.environ.get("TARGET_SAMPLE_SIZE", "100000"),
    },
    **_SPARK_BASE,
)

# ---------------------------------------------------------------------------
# Task 2b — Fault Injection (optional, controlled by FAULT_INJECT env var)
#
# DEFENSE-IN-DEPTH RATIONALE:
# CleanJob already strips passenger_count <= 0 and null PULocationID from
# bronze, so those conditions normally never reach silver.  ValidateJob
# re-checks them on silver to guard against *upstream regressions* — a
# weakened or bypassed CleanJob, a new data source that skips cleaning, etc.
# This task simulates such a regression by writing 3 synthetic bad rows
# directly into the partitioned silver/ directory, bypassing CleanJob.
# ValidateJob then catches them and fails the DAG run — proving the gate
# works end-to-end without touching or corrupting the real bronze data.
#
# Cleanup: the injected file (INJECTED_BAD_ROWS.parquet) is the only artefact
# added; purge_injected_silver.py removes it in one call.
# ---------------------------------------------------------------------------

def _fault_inject_silver(**_context):
    """Write 3 bad rows into silver to trigger a ValidateJob failure."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError(
            "pyarrow not available in the Airflow worker.  "
            "Install it or run scripts/inject_bad_silver.py from inside the "
            "Spark container instead."
        )

    import datetime as dt

    target_dir = pathlib.Path(SILVER_PATH) / _INJECT_PARTITION
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / _INJECT_FILENAME

    schema = pa.schema([
        pa.field("tpep_pickup_datetime",  pa.string()),
        pa.field("tpep_dropoff_datetime", pa.string()),
        pa.field("passenger_count",       pa.int32()),
        pa.field("trip_distance",         pa.float64()),
        pa.field("PULocationID",          pa.int32()),
        pa.field("DOLocationID",          pa.int32()),
        pa.field("payment_type",          pa.string()),
        pa.field("fare_amount",           pa.float64()),
        pa.field("tip_amount",            pa.float64()),
        pa.field("total_amount",          pa.float64()),
        pa.field("pickup_datetime",       pa.timestamp("us")),
        pa.field("dropoff_datetime",      pa.timestamp("us")),
        pa.field("payment_type_desc",     pa.string()),
    ])

    ts = dt.datetime(2021, 1, 15, 10, 0, 0)

    data = {
        "tpep_pickup_datetime":  ["2021-01-15 10:00:00", "2021-01-15 11:00:00", "2021-01-15 12:00:00"],
        "tpep_dropoff_datetime": ["2021-01-15 10:30:00", "2021-01-15 11:30:00", "2021-01-15 12:30:00"],
        "passenger_count":       [0,    1,   -1],   # row 0 & 2: invalid (<=0)
        "trip_distance":         [1.5,  2.3,  0.8],
        "PULocationID":          [100,  None, 200], # row 1: invalid (null)
        "DOLocationID":          [200,  300,  100],
        "payment_type":          ["1",  "1",  "2"],
        "fare_amount":           [8.0,  12.0,  6.5],
        "tip_amount":            [1.0,   2.0,  0.0],
        "total_amount":          [9.5,  14.5,  7.0],
        "pickup_datetime":       [ts,    ts,    ts],
        "dropoff_datetime":      [ts,    ts,    ts],
        "payment_type_desc":     ["Credit card", "Credit card", "Cash"],
    }

    arrays = [pa.array(data[f.name], type=f.type) for f in schema]
    table  = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, str(target_file))

    print(f"[fault_inject_silver] Wrote 3 bad rows → {target_file}")
    print(f"[fault_inject_silver]   Row 0: passenger_count=0  → gate catches <=0")
    print(f"[fault_inject_silver]   Row 1: PULocationID=null  → gate catches nulls")
    print(f"[fault_inject_silver]   Row 2: passenger_count=-1 → gate catches <=0")


fault_inject_task = PythonOperator(
    task_id="fault_inject_silver",
    python_callable=_fault_inject_silver,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task 3 — TaskFlow @task (XCom producer)
# Counts Parquet part-files written by CleanJob and returns the count as an
# XCom value.  Contrasts with SparkSubmitOperator: no cluster needed, pure
# Python, automatic XCom push on return.
# ---------------------------------------------------------------------------


@task(dag=dag)
def count_silver_partitions() -> int:
    """Return the number of Parquet part-files in the silver layer.

    The value is automatically stored as an XCom and consumed by the
    downstream BranchPythonOperator to decide whether to run full analytics
    or short-circuit the pipeline.
    """
    silver = pathlib.Path(SILVER_PATH)
    if not silver.exists():
        print(f"[count_silver_partitions] Silver path '{SILVER_PATH}' not found — returning 0.")
        return 0
    parquet_files = list(silver.rglob("*.parquet"))
    count = len(parquet_files)
    print(f"[count_silver_partitions] Found {count} Parquet part-file(s) under '{SILVER_PATH}'.")
    return count


count_task = count_silver_partitions()

# ---------------------------------------------------------------------------
# Task 4 — BranchPythonOperator (XCom consumer)
# Pulls the partition count from XCom and routes to 'analytics_gold' when
# data volume is sufficient, or to 'insufficient_data' otherwise.
# ---------------------------------------------------------------------------


def _branch_on_volume(ti) -> str:
    """Return the task_id of the branch to follow based on silver volume."""
    count: int = ti.xcom_pull(task_ids="count_silver_partitions")
    print(f"[branch_on_volume] Silver partition-file count (XCom): {count}")
    if count is not None and count >= MIN_PARTITION_FILES:
        print(f"[branch_on_volume] count={count} >= threshold={MIN_PARTITION_FILES} → analytics_gold")
        return "analytics_gold"
    print(f"[branch_on_volume] count={count} < threshold={MIN_PARTITION_FILES} → insufficient_data")
    return "insufficient_data"


branch_task = BranchPythonOperator(
    task_id="branch_on_volume",
    python_callable=_branch_on_volume,
    dag=dag,
)

# Short-circuit path — skipped when data is sufficient.
insufficient_data = EmptyOperator(
    task_id="insufficient_data",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Tasks 5 & 6 — SparkSubmitOperator (analytics + validate, unchanged)
# ---------------------------------------------------------------------------

analytics_task = SparkSubmitOperator(
    task_id="analytics_gold",
    java_class="jobs.AnalyticsJob",
    env_vars={
        "CMS_EPSILON":     os.environ.get("CMS_EPSILON",     "0.0001"),
        "CMS_CONFIDENCE":  os.environ.get("CMS_CONFIDENCE",  "0.95"),
        "CMS_SEED":        os.environ.get("CMS_SEED",        "1"),
    },
    **_SPARK_BASE,
)

validate_task = SparkSubmitOperator(
    task_id="validate_quality_gate",
    java_class="jobs.ValidateJob",
    **_SPARK_BASE,
)

# ---------------------------------------------------------------------------
# Join — both branch paths converge here.
# trigger_rule='none_failed_min_one_success' lets the join fire whether the
# pipeline ran analytics or short-circuited through 'insufficient_data'.
# ---------------------------------------------------------------------------

pipeline_done = EmptyOperator(
    task_id="pipeline_done",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Dependency chain
# Fault-inject task is spliced between clean_silver and count_silver_partitions
# only when FAULT_INJECT=true.  Normal run skips it entirely.
# ---------------------------------------------------------------------------

wait_for_raw_data >> ingest_task >> clean_task

if FAULT_INJECT:
    clean_task >> fault_inject_task >> count_task
else:
    clean_task >> count_task

count_task >> branch_task
branch_task >> analytics_task >> validate_task >> pipeline_done
branch_task >> insufficient_data >> pipeline_done
