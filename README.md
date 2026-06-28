# NYC Taxi Analytics — Spark Pipeline

A containerized, Airflow-orchestrated batch analytics pipeline over NYC Yellow Taxi trip data (2021–2023). Compares exact analytical query results against two probabilistic approximation techniques — Reservoir Sampling (Algorithm R) and Count-Min Sketch (CMS) — across a structured three-layer data architecture.

This is version 2 of the project. Version 1 was a monolithic Scala/Spark application running locally against HDFS. Version 2 migrates that work into a reproducible Docker-based environment with Apache Airflow orchestrating discrete pipeline stages.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
- [Development](#development)
- [Version History](#version-history)
- [Out of Scope](#out-of-scope)
- [License](#license)

---

## Features

- Spark Standalone cluster (1 master + 1 worker) running inside Docker Compose
- Apache Airflow (LocalExecutor) orchestrating four sequential pipeline stages
- Three-layer data architecture: raw ingestion, cleaned/partitioned, and curated analytical outputs
- Exact vs. approximate query comparison using Reservoir Sampling and Count-Min Sketch
- Data quality gate that fails the Airflow DAG run on constraint violations
- Reproducible reservoir sample materialized with a fixed seed
- Parquet storage partitioned by year and month, replacing HDFS
- All dependencies pinned; no `:latest` tags
- `Makefile` targets wrapping common Docker Compose operations

---

## Requirements

| Dependency | Version | Notes |
|---|---|---|
| Docker Engine | >= 24 | With Docker Compose v2 |
| Docker Compose | v2 (plugin) | `docker compose`, not `docker-compose` |
| WSL2 (Windows only) | — | Recommended backend; allocate 10–12 GB via `.wslconfig` |
| Host RAM | >= 16 GB | Spark master + worker + Airflow + Postgres run concurrently |
| sbt | >= 1.9 | Only required if building the JAR outside Docker |
| Java | 17 | Bundled inside the Spark image |

> On Windows, ensure Docker Desktop is configured to use the WSL2 backend. The pipeline has not been tested with the Hyper-V backend.

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Asterinos1/NYC-Taxi-Analytics-Spark.git
cd NYC-Taxi-Analytics-Spark
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Review `.env` and adjust values for your host. The defaults are functional on a 16 GB machine. See [Configuration](#configuration) for all available variables.

### 3. Place the source dataset

Place the raw CSV file at:

```
data/yellow_taxi_combined_2021_2023.csv
```

The `data/` directory is bind-mounted into the Spark containers. It is gitignored and must be populated manually.

### 4. Build the Scala fat JAR

```bash
make build
```

This runs `sbt assembly` inside the `spark-app/` directory and produces:

```
spark-app/target/scala-2.12/nyc-taxi-analytics-assembly-2.0.0.jar
```

The JAR is mounted into the Spark containers at `/opt/spark/jars/`.

### 5. Start the cluster

```bash
make up
```

This brings up: `spark-master`, `spark-worker`, `airflow-scheduler`, `airflow-apiserver`, and `postgres`.

Verify all services are healthy:

```bash
make status
```

---

## Usage

### Triggering the pipeline

1. Open the Airflow web UI at [http://localhost:8082](http://localhost:8082).
2. Log in with the default credentials (`admin` / `admin` on first run — change immediately).
3. Locate the DAG `nyc_taxi_medallion_pipeline` and unpause it.
4. Trigger a manual run using the "Trigger DAG" button.
5. Monitor task execution in the Grid or Graph view.

The pipeline runs stages sequentially:

```
ingest_bronze → clean_silver → analytics_gold → validate_quality_gate
```

Each stage is a separate `spark-submit` call via `SparkSubmitOperator`, targeting the Spark standalone cluster at `spark://spark-master:7077`.

### Spark UI

The Spark Master UI is available at [http://localhost:8080](http://localhost:8080) while the cluster is running. Use it to inspect active applications, executor allocation, and stage execution plans.

### Makefile targets

| Target | Description |
|---|---|
| `make up` | Start all services in detached mode |
| `make down` | Stop and remove containers |
| `make build` | Build Docker images and Scala assembly JAR |
| `make logs` | Stream logs from all services |
| `make status` | Print container status |
| `make clean` | Stop services, remove volumes, and delete all data layer directories |
| `make shell-airflow` | Open a bash shell in the Airflow scheduler container |
| `make shell-spark` | Open a bash shell in the Spark master container |

---

## Configuration

All runtime parameters are read from `.env`. Copy `.env.example` to get started:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW_UID` | `50000` | UID for Airflow file ownership (Linux hosts) |
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:3.2.2` | Airflow image tag |
| `SPARK_IMAGE_NAME` | `apache/spark:3.5.6` | Spark image tag |
| `POSTGRES_IMAGE_NAME` | `postgres:16` | Postgres image tag |
| `POSTGRES_USER` | `airflow` | Airflow metadata DB user |
| `POSTGRES_PASSWORD` | `airflow_password` | Airflow metadata DB password |
| `POSTGRES_DB` | `airflow` | Airflow metadata DB name |
| `SPARK_WORKER_CORES` | `2` | CPU cores allocated to the Spark worker |
| `SPARK_WORKER_MEMORY` | `4G` | Memory allocated to the Spark worker |
| `TARGET_SAMPLE_SIZE` | `100000` | Reservoir sample target row count |
| `CMS_EPSILON` | `0.0001` | Count-Min Sketch error bound |
| `CMS_CONFIDENCE` | `0.95` | Count-Min Sketch confidence level |
| `CMS_SEED` | `1` | Fixed seed for reproducible sampling |

Do not commit `.env`. It is listed in `.gitignore`. Commit only `.env.example`.

---

## Project Structure

```text
NYC-Taxi-Analytics-Spark/
├── Makefile                          # Cluster management commands
├── README.md
├── DESING.md                         # Design specification document
├── CHANGELOG.md                      # v1 → v2 migration notes
├── progress.md                       # Implementation status tracker
├── .env.example                      # Environment variable template
├── .gitignore
│
├── docker/
│   ├── docker-compose.yml            # Full cluster definition
│   └── airflow.Dockerfile            # apache/airflow extended with Java 17 + Spark client
│
├── spark-app/                        # Scala Spark application
│   ├── build.sbt                     # sbt-assembly config; Scala 2.12.18, Spark 3.5.6
│   ├── project/
│   │   ├── build.properties
│   │   └── plugins.sbt
│   └── src/main/scala/
│       ├── jobs/
│       │   ├── IngestJob.scala       # Raw layer: CSV → Parquet
│       │   ├── CleanJob.scala        # Cleaned layer: type casting, partitioning, reservoir sample
│       │   ├── AnalyticsJob.scala    # Analytical layer: 6 queries, exact vs. sample vs. CMS
│       │   └── ValidateJob.scala     # Quality gate: row counts, nulls, range checks
│       └── common/
│           ├── Paths.scala           # Shared path constants
│           └── Schemas.scala         # Shared schema definitions
│
├── airflow/
│   ├── dags/
│   │   └── taxi_pipeline_dag.py      # Pipeline DAG definition
│   └── config/
│
├── data/                             # Gitignored; bind-mounted into Spark containers
│   ├── yellow_taxi_combined_2021_2023.csv  # Source dataset (place manually)
│   ├── bronze/                       # Raw layer (Parquet)
│   ├── silver/                       # Cleaned layer (Parquet, partitioned by year/month)
│   ├── silver_sample/                # Materialized reservoir sample
│   └── gold/                         # Analytical outputs
│
└── docs/
    └── screenshots/                  # Spark UI and Airflow graph captures
```

---

## Pipeline Stages

Each stage is an independent `main` class in the fat JAR, submitted as a separate Airflow task.

### ingest (IngestJob)

Reads the raw CSV from `data/yellow_taxi_combined_2021_2023.csv` and writes it as Parquet to `data/bronze/`. No filtering or business logic is applied at this stage. The raw layer serves as an immutable historical record of the source data.

### clean (CleanJob)

Reads from `data/bronze/` and applies:
- Timestamp parsing and type casting
- Filtering on passenger count, location ID, and date range
- Deduplication
- Payment type description enrichment
- Partitioned Parquet write to `data/silver/` (by `year` and `month`)
- Reservoir sample materialization to `data/silver_sample/` with a fixed seed

The cleaned layer is the single source of truth consumed by all downstream stages.

### analytics (AnalyticsJob)

Reads from `data/silver/` and executes 6 analytical queries. Each query reports three result variants:
- Exact result (full DataFrame scan)
- Reservoir sample estimate
- Count-Min Sketch estimate

Results are written to `data/gold/`. All six queries share one cached DataFrame to avoid redundant scans.

### validate (ValidateJob)

Reads from `data/silver/` and asserts:
- Row count is greater than zero
- Key fields (`pickup_datetime`, `PULocationID`) are non-null
- Passenger count values are positive

If any assertion fails, the job throws a runtime exception, which causes the Airflow task to fail and prevents the DAG from advancing. This stage intentionally runs after `analytics` to also validate the gold output if needed.

---

## Development

### Building the JAR locally

```bash
cd spark-app
sbt assembly
```

Output: `target/scala-2.12/nyc-taxi-analytics-assembly-2.0.0.jar`

### Running a single stage manually

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --class jobs.IngestJob \
  --driver-memory 1g \
  --executor-memory 2g \
  /opt/spark/jars/nyc-taxi-analytics-assembly-2.0.0.jar
```

Replace `jobs.IngestJob` with `jobs.CleanJob`, `jobs.AnalyticsJob`, or `jobs.ValidateJob` for other stages.

### Demonstrating a quality gate failure

Inject invalid rows into `data/silver/` (e.g., rows with `passenger_count <= 0` or null `PULocationID`), then trigger only the `validate_quality_gate` task in Airflow. The task should fail with a logged assertion error and halt the DAG run.

### Resource constraints

The full 2021–2023 dataset is approximately 9 GB. During development, use the provided sample or a 1–2 month subset. Configure Spark memory explicitly:

```bash
# In .env
SPARK_WORKER_MEMORY=4G
SPARK_WORKER_CORES=2
```

Driver and executor memory are configured in the DAG's `SparkSubmitOperator` arguments.

---

## Version History

| Version | Branch | Description |
|---|---|---|
| v1.0.0 | [`archive/v1.0.0`](https://github.com/Asterinos1/NYC-Taxi-Analytics-Spark/tree/archive/v1.0.0) | Monolithic `App.scala` running locally against HDFS. Preserved as-is. |
| v2.0.0 | `main` | Containerized Spark cluster, Airflow orchestration, Parquet storage (this branch). |

The v1 codebase is preserved in full on the `archive/v1.0.0` branch and will not receive further changes.

The three migrations from v1 to v2:
1. **Local → Docker**: all dependencies containerized; nothing installed on the host except Docker.
2. **HDFS → Parquet on shared volume**: HDFS removed entirely.
3. **Monolith → orchestrated stages**: single job decomposed into four independently re-runnable Airflow tasks.

See `CHANGELOG.md` for detailed migration notes.

---

## Out of Scope

The following were deliberately excluded. Each is discussed as a production consideration in the project report.

| Excluded | Reason |
|---|---|
| Kubernetes | Operational overhead without proportional learning benefit at this scale |
| CeleryExecutor / Redis | LocalExecutor is sufficient; Celery adds 3+ containers of RAM |
| Kafka / Structured Streaming | This is a batch pipeline; streaming is evaluated in the report, not built |
| Full CI/CD pipeline | Out of scope for a course project |
| Great Expectations | Lightweight assertions are sufficient at this data scale |

---

## License

TODO: Add license information.
