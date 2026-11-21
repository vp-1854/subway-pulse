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

## Processing Jobs

The processing scripts are CLI-driven and default to local paths under `data/`.

```bash
python -m processing.download_datasets --skip-noaa
python -m processing.profile_data
python -m processing.build_bridge_table
python -m processing.clean_ridership
python -m processing.clean_weather
python -m processing.join_ridership_weather
python -m processing.build_baseline --sink parquet
```

Set `NOAA_TOKEN` before running `processing.download_datasets` without `--skip-noaa`. Use `--sink cassandra` or `--sink parquet,cassandra` for batch jobs after Spark has the Cassandra connector available.

The historical window is one year, 2025-11-01 to 2026-09-30 (set in `processing/config.py` as `DATA_WINDOW_START` / `DATA_WINDOW_END`, or via the `SUBWAY_PULSE_WINDOW_START` / `SUBWAY_PULSE_WINDOW_END` environment variables). The downloader fetches only the `Beginning 2025` ridership dataset inside that window, and NOAA weather for the same dates. Override with `--ridership-since`, `--ridership-until`, `--start-date` and `--end-date`. `processing.clean_ridership` also keeps only that window, and stores `date`, `hour_of_day` and `day_of_week` in NYC local time (`day_of_week` follows Spark: Sunday = 1). If you already downloaded older raw files, delete them or pass `--no-skip-existing` so the downloader does not reuse them.

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
├── processing/      # PySpark batch jobs
├── serving/         # FastAPI app (health check)
├── dashboard/       # Streamlit app (status page)
├── data/            # Bridge table and local data files
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
