# Project Implementation Progress (v2.0.0)

This document tracks the status of the NYC Taxi Analytics Spark Pipeline implementations against the requirements specified in [DESING.md](file:///C:/Users/PC/Documents/GitHub/NYC-Taxi-Analytics-Spark/DESING.md).

---

## 1. Deliverables Checklist & Implementation Status

| Requirement / Deliverable | Status | Details / Implementation Notes |
| :--- | :---: | :--- |
| **Containerized Spark Standalone Cluster** | **Completed** | Mapped in `docker/docker-compose.yml` with `spark-master` and `spark-worker` running Apache Spark `3.5.6`. Resources are explicitly managed to run safely on a 16 GB host. |
| **Airflow Pipeline DAG Orchestration** | **Completed** | Defined in `airflow/dags/taxi_pipeline_dag.py` with tasks: `ingest_bronze` &rarr; `clean_silver` &rarr; `analytics_gold` &rarr; `validate_quality_gate` using `SparkSubmitOperator` against the Spark cluster. |
| **HDFS to Shared Volume Migration** | **Completed** | Fully migrated storage from HDFS to a shared Docker volume (`data/`). The ingestion writes to the raw/landing directory (`data/bronze/`) and cleaning writes partitioned Parquet to the cleaned/prepared directory (`data/silver/`). |
| **Scala Spark App & Build Automation** | **Completed** | Migrated Scala v1 monolith into modular Scala files under `spark-app/src/main/scala/jobs/`. Cleaned and built using `sbt-assembly` to generate a fat JAR. |
| **Preservation of Exact vs. Approximate Comparisons** | **Completed** | Reservoir Sampling (Algorithm R) and Count-Min Sketch (CMS) logic remain intact. Reservoir sample is materialized with a stable seed to ensure consistent downstream metrics. |
| **Data Quality Gate** | **Completed** | Implemented `ValidateJob.scala` which checks for empty Cleaned sets, null values on key fields (`pickup_datetime`, `PULocationID`), and invalid passenger counts (`<= 0`). Fails with runtime exceptions on violations. |
| **Versioned Docker Images & Environments** | **Completed** | Extended the Airflow image in `docker/airflow.Dockerfile` to include Java and Spark client utilities. All images (`apache/spark:3.5.6`, `postgres:16`, etc.) are explicitly pinned. |
| **Local Sandbox Control (Makefile)** | **Completed** | Created a root-level [Makefile](file:///C:/Users/PC/Documents/GitHub/NYC-Taxi-Analytics-Spark/Makefile) providing simple targets to manage container states (`up`, `down`, `build`, `clean`, `logs`, `status`). |
| **Configuration separation** | **Completed** | Clean/Analytics parameters (`TARGET_SAMPLE_SIZE`, `CMS_EPSILON`, etc.) and paths are exposed through environments via `.env` (provided via `.env.example`). |

---

## 2. Component Directory Structure Mapping

- **Infrastructure**:
  - `docker/docker-compose.yml`: Spark standalone + Airflow LocalExecutor + Postgres metadata db.
  - `docker/airflow.Dockerfile`: Custom Airflow runner with Java 17 + Spark submit dependencies.
  - `Makefile`: Shell shortcuts wrapper.
- **Orchestration**:
  - `airflow/dags/taxi_pipeline_dag.py`: Medallion pipeline task flow definition.
- **Compute**:
  - `spark-app/src/main/scala/jobs/`:
    - `IngestJob.scala`: Moves raw data to Raw layer.
    - `CleanJob.scala`: Type parsing, partitions cleaned data, generates fixed reservoir sample.
    - `AnalyticsJob.scala`: Runs 6 core queries comparing exact vs approximate counts on the Analytical layer.
    - `ValidateJob.scala`: Quality assurance assertions gate.
- **Data Layers**:
  - `data/bronze/`: Landing / Raw Layer.
  - `data/silver/`: Cleaned / Prepared Layer (partitioned by `year` and `month`).
  - `data/silver_sample/`: Materialized Reservoir Sample.
  - `data/gold/`: Curated / Analytical Layer outputs.

---

## 3. What's Left to Implement

1. **Demonstrate Quality Gate Failure**:
   - Simulate/inject anomalous data (e.g., negative passenger counts or missing location IDs) into the Cleaned dataset.
   - Trigger the `ValidateJob` and show that it fails the run with the appropriate runtime error as designed, gating the pipeline.

2. **Capture Evidence (Screenshots)**:
   - Export and save screenshots of the Airflow Graph/Grid views showing the pipeline DAG.
   - Export and save screenshots of the Spark UI showing stage plans and execution performance.
   - Place these screenshots under `docs/screenshots/`.

3. **Git Branch Merging & Version Release Tagging**:
   - Stage, commit, and push all untracked files on the `feat/dockerize-airflow` branch.
   - Merge the branch into `main`.
   - Tag the release as `v2.0.0` (or `v2.0-docker-airflow`) and write the `CHANGELOG.md` migration notes.

4. **Prepare Report & Presentation**:
   - Write the pipeline evaluation report (sections covering: problem definition, architecture, Spark execution, Airflow DAG execution, pros/cons comparison, and future scope).
   - Draft the ~8-10 slide presentation deck.

5. **Stretch Goal (Spark MLlib Training Job)**:
   - If time permits, implement the stretch MLlib job (`TrainJob.scala`) to train a simple tip-amount regression model, outputting metrics to `metrics.json` and the model to `gold/model/`.
