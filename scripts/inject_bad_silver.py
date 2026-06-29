#!/usr/bin/env python3
"""
inject_bad_silver.py
====================
Fault-injection helper for the Quality-Gate demo (Goal A).

WHY inject into silver, not bronze?
------------------------------------
CleanJob filters passenger_count <= 0 and null PULocationID/DOLocationID from
bronze before writing silver.  Those bad rows therefore NEVER reach silver under
normal operation, so ValidateJob checking them on silver would always pass on
real data.  To demonstrate a meaningful quality-gate *failure* we must write bad
rows **directly into the partitioned silver/ directory** that ValidateJob reads,
bypassing CleanJob.

This is also the rationale for the gate's existence: it guards against upstream
regressions (e.g. CleanJob is accidentally disabled or its filters are weakened),
not just the current-run data.  Defense in depth, not redundancy.

Usage
-----
  # Inside the airflow-worker or spark container:
  python scripts/inject_bad_silver.py

  # Custom silver path (override default /data/silver):
  SILVER_PATH=/tmp/silver python scripts/inject_bad_silver.py

Cleanup
-------
  python scripts/purge_injected_silver.py

What it writes
--------------
  silver/year=2021/month=1/INJECTED_BAD_ROWS.parquet

  Row 1: passenger_count = 0   (violates ValidateJob assertion 3)
  Row 2: PULocationID = null   (violates ValidateJob assertion 2)
  Row 3: passenger_count = -1  (violates ValidateJob assertion 3)

Any one of these causes ValidateJob to throw RuntimeException and fail the DAG.
"""

import os
import sys

SILVER_PATH = os.environ.get("SILVER_PATH", "/data/silver")
INJECT_PARTITION = "year=2021/month=1"
INJECT_FILENAME = "INJECTED_BAD_ROWS.parquet"

def main():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        sys.exit(
            "[inject_bad_silver] ERROR: pyarrow not installed.\n"
            "  pip install pyarrow   or run inside the Spark container."
        )

    import pathlib

    target_dir = pathlib.Path(SILVER_PATH) / INJECT_PARTITION
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / INJECT_FILENAME

    # ------------------------------------------------------------------
    # Build three bad rows that ValidateJob will catch.
    # Column list mirrors the silver schema written by CleanJob:
    #   tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    #   trip_distance, PULocationID, DOLocationID, payment_type,
    #   fare_amount, tip_amount, total_amount, pickup_datetime,
    #   dropoff_datetime, payment_type_desc, year, month
    # year/month are Hive partition columns — stored in folder name only,
    # not repeated inside the parquet file itself.
    # ------------------------------------------------------------------

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

    import datetime
    ts = datetime.datetime(2021, 1, 15, 10, 0, 0)

    data = {
        "tpep_pickup_datetime":  ["2021-01-15 10:00:00", "2021-01-15 11:00:00", "2021-01-15 12:00:00"],
        "tpep_dropoff_datetime": ["2021-01-15 10:30:00", "2021-01-15 11:30:00", "2021-01-15 12:30:00"],
        "passenger_count":       [0,    1,   -1],   # row 0 & 2 invalid (<=0)
        "trip_distance":         [1.5,  2.3,  0.8],
        "PULocationID":          [100,  None, 200], # row 1 invalid (null)
        "DOLocationID":          [200,  300,  100],
        "payment_type":          ["1",  "1",  "2"],
        "fare_amount":           [8.0,  12.0, 6.5],
        "tip_amount":            [1.0,  2.0,  0.0],
        "total_amount":          [9.5,  14.5, 7.0],
        "pickup_datetime":       [ts,   ts,   ts],
        "dropoff_datetime":      [ts,   ts,   ts],
        "payment_type_desc":     ["Credit card", "Credit card", "Cash"],
    }

    arrays = []
    for field in schema:
        col = data[field.name]
        arrays.append(pa.array(col, type=field.type))

    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, str(target_file))

    print(f"[inject_bad_silver] ✓ Wrote 3 bad rows → {target_file}")
    print(f"[inject_bad_silver]   Row 0: passenger_count=0  (gate catches <=0)")
    print(f"[inject_bad_silver]   Row 1: PULocationID=null  (gate catches nulls)")
    print(f"[inject_bad_silver]   Row 2: passenger_count=-1 (gate catches <=0)")
    print(f"[inject_bad_silver] Run ValidateJob now — it will FAIL as expected.")
    print(f"[inject_bad_silver] Cleanup: python scripts/purge_injected_silver.py")


if __name__ == "__main__":
    main()
