# Subway Pulse

Real-time MTA subway congestion pipeline and live dashboard.

Ingests live GTFS-Realtime feeds, computes per-station congestion scores using a Lambda architecture, and surfaces them on an interactive map.

## Stack

| Layer | Technology |
|---|---|
| Message bus | Apache Kafka |
| Batch + stream processing | PySpark |
| Speed-layer store | MongoDB |
| Batch-layer store | Cassandra |
| Serving | FastAPI |
| Dashboard | Streamlit + PyDeck |

---

## Prerequisites

- Docker Desktop
- Python 3.11 or newer (the pinned `cassandra-driver` 3.29.3 supports 3.12 and 3.13)
- Java 17+ for PySpark
- See `requirements.txt` for pinned dependencies

Install Python dependencies:
```bash
pip install -r requirements.txt
```

---

## Quick Start

**1. Start the full stack** (Kafka, Zookeeper, Spark, Mongo, Cassandra)
```bash
cd infra
docker compose up -d
```
Topics are auto-created on first start by the `kafka-init` one-shot
container. Re-create manually with `./infra/create_topics.sh` if needed.

**2. Initialize MongoDB TTL index** (run once after first `docker compose up`)
```bash
python infra/init_mongo.py
```

**3. Verify connectivity**
```bash
python infra/verify_connections.py
```

**4. Start ingestion**
```bash
python -m ingestion.gtfs_producer    # live MTA → Kafka, every 15s
python -m ingestion.weather_poller   # NWS → Cassandra + Kafka, every 15min
python -m ingestion.gtfs_monitor     # one-line throughput report every 60s
```

**5. Start the serving layer**
```bash
uvicorn serving.main:app --reload
```
API available at `http://localhost:8000`. Check `http://localhost:8000/health` to confirm MongoDB and Cassandra are reachable.

**6. Run the dashboard**
```bash
streamlit run dashboard/app.py
```
Dashboard available at `http://localhost:8501`.

---

## Running Tests

Run local tests:
```bash
python -m unittest discover -v
```

---

## Configuration

All connection settings are read from environment variables:

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `CASSANDRA_HOSTS` | `localhost` | Comma-separated Cassandra host list |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `NWS_STATION` | `KNYC` | NWS station for weather poller (Central Park ASOS) |
| `SUBWAY_PULSE_DATA_DIR` | `./data` | Local data dir for Parquet sinks |
| `SUBWAY_PULSE_CHECKPOINT_DIR` | `./checkpoints` | Spark Structured Streaming checkpoints |

---

## Project Structure

```
subway-pulse/
├── infra/           # Docker Compose, DB init scripts
├── ingestion/       # Kafka producers, GTFS + weather pollers
├── processing/      # Shared config, Spark helper, score formulas
├── serving/         # FastAPI app (health check)
├── dashboard/       # Streamlit app (status page)
└── docs/            # Design notes and schemas
```

---

## Operations

| Task | Command |
|---|---|
| Inspect Kafka topics | `docker exec -it subway_kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| Check consumer lag | `docker exec -it subway_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups` |
| Tail DLQ | `docker exec -it subway_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gtfs-dlq --from-beginning --max-messages 10` |
| Spark UI | <http://localhost:8080> |

See [`docs/kafka_config.md`](docs/kafka_config.md) for partition layout, consumer
groups, and tuning. See [`docs/mta_feeds.md`](docs/mta_feeds.md) for the MTA
feed URL list.

---

## Data Sources

- **MTA GTFS-Realtime** — live vehicle positions and trip updates (public, no API key required)
- **MTA Hourly Ridership** — historical tap-in counts per station (NY Open Data)
- **MTA Stations Dataset** — station complex metadata and GTFS stop mappings (NY Open Data)
- **NOAA GHCND** — historical daily weather for baseline computation
- **NWS API** — real-time current weather (`api.weather.gov`, no API key required)
