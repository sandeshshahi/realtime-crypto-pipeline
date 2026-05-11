# Real-Time Multi-Asset Crypto Pipeline & Analytics

## Distributed Big Data Architecture Overview

This project implements a production-grade, end-to-end Big Data pipeline designed to ingest, process, enrich, and visualize high-frequency cryptocurrency trade data in real time. The architecture leverages a containerized environment to orchestrate multiple distributed systems, ensuring fault tolerance, seamless service discovery via Docker bridge networks, and scalability.

---

## Core Features

### Real-Time Data Ingestion (Apache Kafka)

- **Data Source:** Connects to the Binance.US WebSocket API (`wss://stream.binance.us:9443`).
- **Combined Streams:** Multiplexes connections to ingest live trade data for various crypto pairs (BTC, ETH, SOL, XRP, BNB, ADA) simultaneously through a single persistent connection to reduce network overhead.
- **Producer Logic:** A robust Python script utilizing the `kafka-python` library for broker communication and `websocket-client` for real-time streaming.
- **Message Delivery:** Asynchronously publishes raw JSON trade data into an Apache Kafka topic named `crypto_trades`.

### Distributed Processing (Spark Structured Streaming)

- **Engine:** Utilizes Apache Spark 3.x with Structured Streaming to subscribe to the Kafka topic.
- **Real-Time Transformations:** Calculates meaningful aggregations, specifically the `average_price` and `total_volume` over 1-minute tumbling windows.
- **Adapter Integration:** The `spark-submit` command passes a comma-separated list to the `--packages` flag to dynamically provision both the Kafka adapter and the Cassandra connector.
- **State Management:** Implements event-time watermarking to handle late-arriving data and prevent state memory bloat.

### Real-Time Anomaly Detection (Volatility Tracking)

- **Streaming-Safe Math:** Calculates the high/low price spread natively within Spark's tumbling windows to avoid the memory overhead of non-time-based windowing functions.
- **Statistical Flagging:** Computes the `price_swing_pct` for every asset, every minute. If an asset's price fluctuates by more than 1.5% within a single 60-second window, the pipeline triggers an `is_anomaly = True` boolean flag.
- **Optimized Storage:** Stores anomalies as highly-indexable booleans rather than text strings, enabling rapid NoSQL querying and live "Alert Feeds" in the visualization layer.

### Data Enrichment with Spark SQL (HDFS Integration)

- **Static Reference Data:** A static CSV file containing asset metadata (e.g., Full Name, Category, Market Type) is stored within the Hadoop Distributed File System (HDFS).
- **Stream-Static Join:** The Spark application performs a live join between the streaming Kafka DataFrame and the static HDFS dataset using Spark SQL.
- **Outcome:** Raw transaction symbols (e.g., `BTCUSDT`) are enriched with human-readable context (e.g., `Bitcoin`, `Layer 1`) before being persisted.

### Persistent Storage (Apache Cassandra)

- **Sink Integration:** Processed DataFrames are written to a persistent NoSQL storage layer optimized for time-series data using Spark's `foreachBatch` logic.
- **Schema Management:** Includes a `cassandra/schema.cql` file for automated keyspace (`cryptopulse`) and table (`real_time_aggregates`) initialization.
- **Docker Volumes:** Data is persisted on the local host machine, ensuring database records survive container restarts and pipeline shutdowns.

### Visualization & Dashboarding (Grafana)

- **Zero-Touch Provisioning (IaC):** Employs Grafana provisioning via YAML configurations and Docker volume mounts to automatically initialize the Cassandra data source (forcing LOCAL_ONE consistency and hardcoded UIDs to prevent state ghosts) and load pre-built JSON dashboards on container startup.
- **Separation of Concerns:** Features two dedicated dashboards: a Crypto Stream dashboard for raw metric monitoring, and a Log-Scale Pricing dashboard to accurately compare high-value assets (Bitcoin) against lower-value assets (ADA/XRP) on the same Y-axis.
- **Live Dashboard:** Connects Cassandra to Grafana 10.x using the `hadesarchitect-cassandra-datasource` plugin.
- **Logarithmic Scaling:** Employs base-10 logarithmic scaling to accurately compare high-value assets (Bitcoin) against lower-value assets (ADA/XRP) on the same Y-axis.
- **Advanced Data Transformations:** Uses a transformation pipeline consisting of Merge series, Sort by (time), and Partition by values to format raw NoSQL multi-frame data into clean, continuous time-series visual lines.

### Full Observability Stack (Prometheus)

- **Health Monitoring:** Integrates Prometheus to scrape metrics and monitor infrastructure health.
- **Dynamic Configuration:** Uses Docker bind mounts for `prometheus.yml`, allowing real-time configuration updates without container rebuilds.

### Infrastructure Automation & Orchestration

- **Cross-Platform Orchestrator:** The start_pipeline.sh script features dynamic OS-detection (Mac/Linux) to automatically locate, verify, and enforce Java 17 requirements, overriding system PATH variables to prevent JVM security crashes (getSubject exceptions) without manual configuration.
- **Docker Networking & DNS Bypass:** Enforces network isolation and automatic service discovery by placing all containers (Zookeeper, Kafka, Cassandra, Hadoop, Prometheus, Grafana) on a dedicated `big-data-net` bridge network.Forces HDFS to map via localhost:9000 for host-side Spark execution, elegantly bypassing Docker internal DNS restrictions on Mac environments.
- **Idempotent Initialization:** The script automatically uploads HDFS files, clears stale Spark checkpoints, creates Kafka topics, and builds Cassandra schemas before launching the jobs.
- **Graceful Shutdowns:** Traps `SIGINT` (Ctrl+C) signals to simultaneously and safely terminate both the Python producer and the Spark Streaming engine.

---

## Configuration (.env)

The project reads runtime settings from a `.env` file at the repository root. Ensure these values are set before running the pipeline.

```text
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=crypto_trades
BINANCE_WS_URL=wss://stream.binance.us:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade/bnbusdt@trade/adausdt@trade/xrpusdt@trade
```

---

## How to Run the Pipeline

**1. Start the Infrastructure**
Launch the decoupled containerized environment:

```bash
docker-compose up -d
```

**2. Prepare the Python Environment**
Activate your virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**3. Execute the Zero-Touch Pipeline Orchestrator**
Run the custom Bash script. It will automatically detect your OS, enforce Java 17, upload the static CSV to Hadoop, build the Cassandra schema, and launch both the producer and streaming engine:

```bash
chmod +x start_pipeline.sh
./start_pipeline.sh
```

**5. View the Dashboard**

Navigate to http://localhost:3000

Login with

- **Username:** admin

- **Password:** admin

Go to Dashboards to view the auto-provisioned analytics:

- **Crypto Prices (Log Scale)**
- **Crypto Stream Analytics**

---

## Troubleshooting & Operations

Spark ↔ Kafka Connectivity: When running Spark locally on the host, use localhost:9092. If containerizing the Spark job, use the Docker bridge network DNS: kafka:29092.

**Verify Database Records:**

```bash
docker exec -it cassandra cqlsh
USE cryptopulse;
SELECT * FROM real_time_aggregates;
```

**Clear Spark Checkpoints (Development Only):**

```bash
rm -rf /tmp/spark_checkpoints_crypto
```

**Safely Stop the Infrastructure (Data remains saved in volumes):**

```bash
docker-compose stop
```
