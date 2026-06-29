#!/usr/bin/env python3
"""
purge_injected_silver.py
========================
Removes the synthetic bad rows injected by inject_bad_silver.py so the
normal, passing pipeline run is unaffected.

Usage
-----
  python scripts/purge_injected_silver.py

  # Custom silver path:
  SILVER_PATH=/tmp/silver python scripts/purge_injected_silver.py
"""

import os
import pathlib

SILVER_PATH = os.environ.get("SILVER_PATH", "/data/silver")
INJECT_PARTITION = "year=2021/month=1"
INJECT_FILENAME  = "INJECTED_BAD_ROWS.parquet"


def main():
    target = pathlib.Path(SILVER_PATH) / INJECT_PARTITION / INJECT_FILENAME

    if not target.exists():
        print(f"[purge_injected_silver] Nothing to remove — {target} not found.")
        return

    target.unlink()
    print(f"[purge_injected_silver] ✓ Removed {target}")
    print(f"[purge_injected_silver] Silver layer restored. Normal pipeline run is safe.")


if __name__ == "__main__":
    main()
