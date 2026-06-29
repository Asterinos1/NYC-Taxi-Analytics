# Project Implementation Progress (v2.3.0)

This document tracks the status of the NYC Taxi Analytics Spark Pipeline implementations against the requirements specified in [DESING.md](file:///C:/Users/PC/Documents/GitHub/NYC-Taxi-Analytics-Spark/DESING.md).

---

## 1. Deliverables Checklist & Implementation Status

| Requirement / Deliverable | Status | Details / Implementation Notes |
| :--- | :---: | :--- |
| **Containerized Spark Standalone Cluster** | **Completed** | Mapped in `docker/docker-compose.yml` with `spark-master` and `spark-worker` running Apache Spark `3.5.6`. Includes **Spark History Server** (`spark-history-server` service) on port `18080` for persistent event logging. |
| **Airflow Pipeline DAG Orchestration** | **Completed** | Defined in `airflow/dags/taxi_pipeline_dag.py`. Tasks: `wait_for_raw_data` (FileSensor) &rarr; `ingest_bronze` &rarr; `clean_silver` &rarr; `analytics_gold` &rarr; `validate_quality_gate` &rarr; `pipeline_done`. Supports `FAULT_INJECT` mode. |
| **Multi-File Ingestion (2020-2023)** | **Completed** | Upgraded `Paths.scala`, `CleanJob.scala`, and Airflow configuration to support directory-based ingestion of multi-year CSV datasets (`yellow_taxi_2020.csv` to `yellow_taxi_2023.csv`) starting from `2020-01-01`. |
| **Scala Spark App & Build Automation** | **Completed** | Modularized jobs under `spark-app/src/main/scala/jobs/`. Cleaned and packaged using IntelliJ's `sbt-launch.jar` to resolve local PATH limitations. |
| **Three-way Estimation & Algorithm Fixes** | **Completed** | Fixed the approximate methods: true reservoir sampling (Algorithm R via deterministic `orderBy(rand).limit()`), resolved string representation bugs in CMS key parsing (Q4/Q6), fixed composite key delimiter collisions (`_` to `|`), and matched Q2 CMS domain logic. |
| **Execution Metrics & Error Margins** | **Completed** | Added programmatic timing (forcing materialization via `cache().count()`) and exact/approx error margin calculations in `AnalyticsJob.scala`. Results are output to the `/data/gold/metrics.json` JSON artifact. |
| **Data Quality Gate & Fault Injection** | **Completed** | Implemented `ValidateJob.scala` to assert silver constraints. Spliced the `fault_inject_silver` Python task into the Airflow DAG (controlled by `FAULT_INJECT=true` flag) to test/prove quality gate failure end-to-end. |
| **Shared Persistent Volume Storage** | **Completed** | Fully migrated to shared Docker volume (`data/`). Shared `data/spark-events` across Spark master, worker, and history server to persist logging data. |
| **Configuration separation** | **Completed** | Separated environments using `.env` for variables such as `CMS_EPSILON`, `CMS_CONFIDENCE`, and connection configurations (`AIRFLOW_CONN_FS_DEFAULT=fs://`). |

---

## 2. Component Directory Structure Mapping

- **Infrastructure & Automation**:
  - `docker/docker-compose.yml`: Spark Standalone Master/Worker + Spark History Server + Airflow LocalExecutor + Postgres DB.
  - `docker/airflow.Dockerfile`: Custom Airflow runner with Java 17 + Spark submit dependencies.
  - `Makefile`: Shell shortcuts wrapper.
  - `scripts/run_and_capture.ps1`: Automated pipeline execution, status polling, metrics harvester, and screenshot capture manager.
  - `scripts/capture_screenshots.ps1`: Automated PowerShell browser screenshot capture tool for UIs.
  - `scripts/inject_bad_silver.py`: Fault injection helper script for data quality testing.
  - `scripts/purge_injected_silver.py`: Cleanup utility for fault injection test cases.
- **Orchestration**:
  - `airflow/dags/taxi_pipeline_dag.py`: Medallion pipeline task flow definition.
  - `airflow/config/webserver_config.py`: Custom webserver configuration supporting anonymous Admin access.
- **Compute**:
  - `spark-app/src/main/scala/jobs/`:
    - `IngestJob.scala`: Ingests multi-file raw CSVs into Bronze Parquet.
    - `CleanJob.scala`: Cleans raw records, partitions silver by `year`/`month`, and writes a fixed reservoir sample.
    - `AnalyticsJob.scala`: Benchmarks Exact, Reservoir, and CMS algorithms.
    - `ValidateJob.scala`: Quality assurance assertions gate.
- **Data Layers**:
  - `data/raw/`: Landing zone for raw yearly CSVs (2020-2023).
  - `data/spark-events/`: Persistent event log directory for Spark History Server.
  - `data/bronze/`: Landing / Raw Layer Parquet.
  - `data/silver/`: Cleaned / Prepared Layer Parquet (partitioned by `year` and `month`).
  - `data/silver_sample/`: Materialized true Reservoir Sample.
  - `data/gold/`: Curated Analytical Layer outputs.
  - `data/gold/metrics.json`: Captured timing, error margin, and performance metrics artifact.

---

## 3. What's Left to Implement

1. **Capture Evidence (Screenshots)**:
   - Run the automated `scripts/capture_screenshots.ps1` script to export and save screenshots of the Spark UI, Spark History Server, and Airflow Graph/Grid views showing successful execution.
   - Place and verify these screenshots under `docs/screenshots/`.

2. **Version Release Tagging & Changelog**:
   - Create tag `v2.3.0` for the release on the `main` branch (branch `feat/dockerize-airflow` promotion is completed).
   - Write the `CHANGELOG.md` migration notes.

3. **Prepare Report & Presentation**:
   - Write the final pipeline evaluation report based on the captured `data/gold/metrics.json`.
   - Draft the presentation slides.
