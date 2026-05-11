# Market Intelligence Data Platform

A real-time financial market data pipeline that ingests simulated stock market tick data, processes it through a multi-layer streaming and batch architecture, and serves analytics via a REST API and interactive dashboard.

Built with **Apache Kafka**, **PySpark Structured Streaming**, **Snowflake**, **dbt**, **FastAPI**, **Redis**, **Apache Superset**, and **Apache Airflow** — fully containerized with Docker Compose and automated with GitHub Actions CI/CD.

---

## Architecture

```
Simulated Market Data (Apple, Google, Microsoft, Amazon, Tesla)
                │
                ▼
    Apache Kafka (market.trades / market.quotes)
                │
                ▼
    PySpark Structured Streaming
    (VWAP · Rolling Averages · Anomaly Detection)
                │
                ▼
        Snowflake — Raw Layer
                │
                ▼
          dbt Transformations
      (raw → staging → marts)
                │
        ┌───────┴────────┐
        ▼                ▼
  FastAPI + Redis    Apache Superset
  (Analytics API)    (Dashboard)

  Apache Airflow → Pipeline monitoring every 15 min
  GitHub Actions → CI/CD on every push to main
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka, Python producer |
| Stream Processing | Apache Spark Structured Streaming (PySpark) |
| Cloud Warehouse | Snowflake |
| Data Modeling | dbt (raw → staging → marts) |
| Serving API | FastAPI + Redis cache |
| Dashboard | Apache Superset |
| Orchestration | Apache Airflow |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |
| Testing | pytest, flake8 |

---

## Screenshots

### Kafka UI — Real-time topic monitoring
![Kafka UI](screenshots/Kafka%20UI%20-%20market.trades%20and%20market.quotes%20topics.png)

### Snowflake — Raw trade metrics table
![Snowflake](screenshots/Snowflake%20-%20Raw%20Trade%20Metrics%20Table.png)

### dbt — Data lineage graph
![dbt Lineage](screenshots/dbt%20Lineage%20Graph.png)

### FastAPI — Interactive API documentation
![FastAPI](screenshots/FastAPI%20-%20Market%20Intelligence%20API%20Endpoints.png)

### Apache Superset — Analytics dashboard
![Superset Dashboard](screenshots/Apache%20Superset%20Dashboard%20.png)

### Apache Airflow — DAG graph view
![Airflow DAG](screenshots/Airflow%20DAG%20-%20market_intelligence_pipeline.png)

### GitHub Actions — CI pipeline
![GitHub Actions](screenshots/GitHub%20Actions%20green%20CI%20run.png)

---

## Project Structure

```
market-intelligence-platform/
├── producer/
│   └── market_producer.py          # Simulates market tick data → Kafka
├── spark_jobs/
│   ├── stream_processor.py         # PySpark Structured Streaming job
│   └── snowflake_writer.py         # Writes Spark output to Snowflake
├── dbt_project/
│   └── market_intelligence/
│       └── models/
│           ├── staging/             # stg_trades, stg_trade_metrics
│           └── marts/               # mart_symbol_performance, mart_anomalies
├── api/
│   └── main.py                     # FastAPI endpoints with Redis caching
├── dags/
│   └── market_intelligence_dag.py  # Airflow monitoring DAG
├── tests/
│   └── test_producer.py            # Unit tests
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI pipeline
├── screenshots/                    # Project screenshots for README
├── Dockerfile.superset             # Custom Superset image with Snowflake driver
├── docker-compose.yml              # All services
└── requirements.txt
```

---

## Data Flow

### 1. Ingestion — Apache Kafka
A Python producer simulates realistic market tick data for 5 symbols with random price walks and bid/ask spreads. Events are published to two Kafka topics every 10 seconds:
- `market.trades` — price, size, exchange per trade
- `market.quotes` — bid/ask prices and sizes

### 2. Stream Processing — PySpark
A PySpark Structured Streaming job consumes from `market.trades` and computes per-symbol rolling metrics over 1-minute windows every 30 seconds:
- **VWAP** (Volume Weighted Average Price)
- Average, min, max price
- Total volume and trade count
- Price volatility percentage

### 3. Cloud Warehouse — Snowflake
Processed data lands in Snowflake across three schema layers:
- `raw` — direct output from Spark (trades, quotes, trade_metrics)
- `staging` — cleaned and validated views via dbt
- `marts` — business-ready aggregated tables via dbt

### 4. Data Modeling — dbt
Four models with tests and full lineage tracking:
- `stg_trades` — filters invalid prices and sizes, computes trade value
- `stg_trade_metrics` — adds price range and volatility calculations
- `mart_symbol_performance` — hourly performance summary per symbol
- `mart_anomalies` — flags prices more than 2 standard deviations from mean VWAP

### 5. Serving Layer — FastAPI + Redis
7 REST endpoints with 60-second Redis cache TTL:

| Endpoint | Description |
|---|---|
| `GET /` | Health check |
| `GET /health` | API status |
| `GET /symbols` | All tracked symbols |
| `GET /performance` | Hourly performance all symbols |
| `GET /performance/{symbol}` | Per symbol performance |
| `GET /anomalies` | Detected price anomalies |
| `GET /latest-trades` | Latest price per symbol |

### 6. Dashboard — Apache Superset
Connected to Snowflake marts with 4 charts on a single dashboard:
- Average VWAP by Symbol
- Total Volume by Symbol
- Price Range by Symbol
- Volatility by Symbol

### 7. Orchestration — Apache Airflow
DAG runs every 15 minutes with 4 tasks in sequence:

```
check_snowflake_connection
        │
        ▼
check_data_freshness
        │
        ▼
validate_trade_metrics
        │
        ▼
run_dbt_models
```

---

## Setup & Installation

### Prerequisites
- Python 3.12+
- Docker Desktop
- Snowflake account (free 30-day trial at snowflake.com)

### 1. Clone the repo

```bash
git clone https://github.com/janhavijadhav/market-intelligence-platform.git
cd market-intelligence-platform
```

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=pipeline_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=market_intelligence
SNOWFLAKE_SCHEMA=raw
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

REDIS_HOST=localhost
REDIS_PORT=6379
```

### 4. Start all services

```bash
docker compose up -d
```

### 5. Run the producer

```bash
python producer/market_producer.py
```

### 6. Run Spark Streaming

```bash
python spark_jobs/stream_processor.py
```

### 7. Run the API

```bash
uvicorn api.main:app --reload --port 8001
```

### 8. Run dbt models

```bash
cd dbt_project/market_intelligence
dbt run
dbt test
dbt docs serve --port 8085
```

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Apache Superset | http://localhost:8088 | admin / admin123 |
| FastAPI Docs | http://localhost:8001/docs | — |
| Apache Airflow | http://localhost:8090 | admin / admin123 |

---

## Running Tests

```bash
python -m pytest tests/test_producer.py -v
```

7 unit tests covering trade simulation, quote generation, price validation and symbol coverage.

---

## CI/CD Pipeline

GitHub Actions runs automatically on every push to `main` with two jobs:

**test job:**
1. Install Python 3.12 and dependencies
2. Run pytest unit tests
3. Lint with flake8

**docker job** (runs after test passes):
1. Build Docker images via Docker Compose

---

## Key Concepts Demonstrated

- **Event-driven architecture** — decoupled producer/consumer via Kafka topics
- **Micro-batch streaming** — 30-second trigger intervals with watermarking for late data
- **VWAP computation** — industry-standard volume-weighted price metric
- **Lakehouse pattern** — raw → staging → marts layer separation
- **Data lineage** — end-to-end lineage tracked automatically by dbt
- **Caching strategy** — Redis TTL cache reduces Snowflake query load
- **Pipeline observability** — Airflow monitors data freshness and null checks
- **Anomaly detection** — statistical z-score based price spike detection
