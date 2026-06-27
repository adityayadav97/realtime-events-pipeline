# ⚡ Real-Time Events Pipeline

> A streaming data pipeline that ingests high-volume clickstream events from **Apache Kafka**, processes them in real time with **Spark Structured Streaming**, lands them in **Delta Lake** (exactly-once), and serves hourly analytics marts via **dbt** — all orchestrated and monitored with **Apache Airflow**.

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white">
  <img alt="Apache Kafka" src="https://img.shields.io/badge/Apache%20Kafka-3.6-231F20?logo=apachekafka&logoColor=white">
  <img alt="Spark Streaming" src="https://img.shields.io/badge/Spark%20Structured%20Streaming-3.5-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Delta Lake" src="https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4">
  <img alt="dbt" src="https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white">
  <img alt="Airflow" src="https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="License" src="https://img.shields.io/badge/License-MIT-green">
</p>

---

## 📌 Why this project

Most data engineers can build batch pipelines — fewer can build **reliable streaming** ones. This project demonstrates real-time ingestion with **exactly-once semantics**, **checkpointing**, **watermarking** for late data, and **stateful aggregation** — the hard parts of streaming.

It complements my batch project ([retail-lakehouse-pipeline](https://github.com/adityayadav97/retail-lakehouse-pipeline)) to show the full data engineering spectrum.

---

## 🏗️ Architecture

```
   ┌────────────────┐     events      ┌──────────────────────────┐    exactly-once    ┌───────────────┐
   │  Event Producer│ ──────────────▶ │  Spark Structured        │ ─────────────────▶ │  Delta Lake   │
   │  (Kafka topic: │   JSON @ N/sec  │  Streaming               │   checkpoint +     │  events_raw   │
   │  user_events)  │                 │  (parse, validate,       │   watermark        │  (append)     │
   └────────────────┘                 │   dedupe, watermark)     │                    └───────────────┘
                                       └──────────────────────────┘                           │
                                                                                              ▼
                          ┌──────────────────────────────────────────────────────────────────────────┐
                          │  Airflow: hourly batch rollup  →  Delta events_hourly  →  dbt marts        │
                          └──────────────────────────────────────────────────────────────────────────┘
```

---

## 🧰 Tech Stack

- **Streaming:** Apache Kafka 3.6, Spark Structured Streaming 3.5
- **Storage:** Delta Lake (exactly-once sink, checkpointing)
- **Late-data handling:** event-time watermarking + deduplication
- **Batch + Modeling:** PySpark hourly rollup, dbt marts
- **Orchestration:** Apache Airflow 2.8
- **Local infra:** Docker Compose (Kafka + Zookeeper)

---

## 📂 Project Structure

```
realtime-events-pipeline/
├── src/
│   ├── config.py
│   ├── producer/event_producer.py     # simulates clickstream into Kafka
│   ├── streaming/stream_consumer.py    # Spark Structured Streaming → Delta
│   └── batch/hourly_rollup.py          # hourly aggregation → Delta
├── dags/streaming_monitor_dag.py       # Airflow: rollup + dbt + freshness check
├── dbt/models/marts/fct_hourly_events.sql
├── tests/test_event_schema.py
├── docker-compose.yml                  # Kafka + Zookeeper
├── requirements.txt
└── Makefile
```

---

## 🚀 Quick Start

```bash
git clone https://github.com/adityayadav97/realtime-events-pipeline.git
cd realtime-events-pipeline
pip install -r requirements.txt

# 1. Start Kafka locally
docker compose up -d

# 2. Start the event producer (terminal 1)
python -m src.producer.event_producer --rate 50

# 3. Start the streaming consumer (terminal 2)
python -m src.streaming.stream_consumer

# 4. Run the hourly rollup + dbt
python -m src.batch.hourly_rollup
cd dbt && dbt build --profiles-dir .
```

---

## 🔑 Streaming Concepts Demonstrated

| Concept | How it's used |
|---------|---------------|
| **Exactly-once** | Delta sink + Spark checkpointing guarantees no duplicates / no loss |
| **Watermarking** | Late events tolerated up to 10 minutes via `withWatermark` |
| **Deduplication** | `dropDuplicates` on `event_id` within the watermark window |
| **Stateful aggregation** | Windowed event counts per event type |
| **Schema enforcement** | Explicit schema applied to Kafka JSON payloads |
| **Backpressure** | `maxOffsetsPerTrigger` to bound micro-batch size |

---

## 🧪 Testing

```bash
pytest tests/ -v
```

---

## 💡 Key Engineering Decisions

- **Delta as the streaming sink** — gives ACID guarantees and exactly-once without extra infrastructure.
- **Event-time over processing-time** — correct results even when events arrive out of order.
- **Separation of stream + batch** — the stream lands raw events fast; batch rollups and dbt build analytics, keeping each layer simple and replayable.

---

## 📜 License

MIT © Aditya Yadav
