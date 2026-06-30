# NYC Taxi Analytics — Version 2 Overview

This document summarizes the structural and architectural improvements introduced in **v2** of the NYC Taxi Analytics pipeline.

## Architectural Changes (v1 vs. v2)

* **Local Monolith to Containerized Cluster:** 
  * Compiles Scala code into a unified assembly JAR.
  * Deploys compute jobs onto a containerized Apache Spark standalone cluster (1 Master + 1 Worker) running official Docker images (`apache/spark:3.5.6`).
* **HDFS to Shared Volume Parquet Storage:** 
  * Removed all HDFS dependencies.
  * Implemented a shared Docker volume containing structured Medallion architecture directories:
    * `Bronze/`: Raw dataset loaded directly into Parquet format.
    * `Silver/`: Enriched, filtered, and cleaned Parquet data partitioned by `year` and `month`. Contains a reproducible reservoir sample (100k rows) with a fixed seed.
    * `Gold/`: Query-specific comparison matrices and computed performance metrics.
* **Orchestration with Apache Airflow 3:** 
  * Replaced manual execution scripts with a fully automated DAG lifecycle.
  * Implemented FileSensors to block pipeline ingest until source data mounts.
  * Integrated XCom routing and Branching to direct workflow execution dynamically.
* **Active Quality Gate Validation:** 
  * Spliced a validator step (`ValidateJob`) to enforce schema invariants, null checks, and coordinate verification before analytic queries run.
  * Added environment toggles (`FAULT_INJECT=true`) to demonstrate pipeline short-circuits on corrupt data inputs.
