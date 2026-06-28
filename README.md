# \u26A1 Real-Time Events Pipeline

> A streaming data pipeline that ingests **10K+ clickstream events/min** from **Apache Kafka**, processes them in real time with **Spark Structured Streaming**, lands them in **Delta Lake** (exactly-once), and serves hourly analytics marts via **dbt** \u2014 all orchestrated and monitored with **Apache Airflow**, at **sub-minute event-to-insight latency**.

`Python` \u00b7 `Apache Kafka` \u00b7 `Spark Streaming` \u00b7 `Delta Lake` \u00b7 `dbt` \u00b7 `Airflow` \u00b7 `MIT License`

---

## \U0001F4CC Why this project

Most data engineers can build batch pipelines \u2014 fewer can build **reliable streaming** ones. This project demonstrates real-time ingestion with **exactly-once semantics**, **checkpointing**, **watermarking** for late data, and **stateful aggregation** \u2014 the hard parts of streaming.

It complements my batch project (retail-lakehouse-pipeline) to show the full data engineering spectrum.

---

## \u2699\uFE0F Scale & Performance

| Aspect | Value |
| --- | --- |
| Ingest throughput | **10K+ events/min** (default 200 events/sec \u2248 12K/min) |
| Latency | **Sub-minute** event-to-insight (30s micro-batch trigger) |
| Delivery guarantee | **Exactly-once** (Delta sink + checkpointing) |
| Late data | Tolerated up to 10 min via event-time watermarking |
| Backpressure | Bounded micro-batches (`maxOffsetsPerTrigger = 20,000`) |

---

## \U0001F3D7\uFE0F Architecture

```
  Event Producer (Kafka topic: user_events)  \u2014 JSON @ 200/sec (\u2248 12K/min)
            \u2502
            \u25BC
  Spark Structured Streaming  (parse, validate, dedupe, watermark)
            \u2502  exactly-once (checkpoint + watermark)
            \u25BC
  Delta Lake  events_raw (append)
            \u2502
            \u25BC
  Airflow: hourly rollup \u2192 Delta events_hourly \u2192 dbt marts \u2192 freshness check
```

---

## \U0001F9F0 Tech Stack

* **Streaming:** Apache Kafka 3.6, Spark Structured Streaming 3.5
* **Storage:** Delta Lake (exactly-once sink, checkpointing)
* **Late-data handling:** event-time watermarking + deduplication
* **Batch + Modeling:** PySpark hourly rollup, dbt marts
* **Orchestration:** Apache Airflow 2.8
* **Local infra:** Docker Compose (Kafka + Zookeeper)

---

## \U0001F4C2 Project Structure

```
realtime-events-pipeline/
\u251C\u2500\u2500 src/
\u2502   \u251C\u2500\u2500 config.py
\u2502   \u251C\u2500\u2500 producer/event_producer.py     # simulates 10K+ events/min into Kafka
\u2502   \u251C\u2500\u2500 streaming/stream_consumer.py    # Spark Structured Streaming \u2192 Delta
\u2502   \u2514\u2500\u2500 batch/hourly_rollup.py          # hourly aggregation \u2192 Delta
\u251C\u2500\u2500 dags/streaming_monitor_dag.py       # Airflow: rollup + dbt + freshness check
\u251C\u2500\u2500 dbt/models/marts/fct_hourly_events.sql
\u251C\u2500\u2500 tests/test_event_schema.py
\u251C\u2500\u2500 docker-compose.yml                  # Kafka + Zookeeper
\u251C\u2500\u2500 requirements.txt
\u2514\u2500\u2500 Makefile
```

---

## \U0001F680 Quick Start

```bash
git clone https://github.com/adityayadav97/realtime-events-pipeline.git
cd realtime-events-pipeline
pip install -r requirements.txt

# 1. Start Kafka locally
docker compose up -d

# 2. Start the event producer (terminal 1) \u2014 defaults to 10K+ events/min
python -m src.producer.event_producer            # 200/sec \u2248 12K/min
python -m src.producer.event_producer --rate 50  # lighter local run

# 3. Start the streaming consumer (terminal 2)
python -m src.streaming.stream_consumer

# 4. Run the hourly rollup + dbt
python -m src.batch.hourly_rollup
cd dbt && dbt build --profiles-dir .
```

---

## \U0001F511 Streaming Concepts Demonstrated

| Concept | How it's used |
| --- | --- |
| **Exactly-once** | Delta sink + Spark checkpointing guarantees no duplicates / no loss |
| **Watermarking** | Late events tolerated up to 10 minutes via `withWatermark` |
| **Deduplication** | `dropDuplicates` on event_id within the watermark window |
| **Stateful aggregation** | Windowed event counts per event type |
| **Schema enforcement** | Explicit schema applied to Kafka JSON payloads |
| **Backpressure** | `maxOffsetsPerTrigger` to bound micro-batch size at 10K+/min |
| **Sub-minute latency** | 30-second micro-batch trigger for near-real-time marts |

---

## \U0001F9EA Testing

```bash
pytest tests/ -v
```

---

## \U0001F4A1 Key Engineering Decisions

* **Delta as the streaming sink** \u2014 gives ACID guarantees and exactly-once without extra infrastructure.
* **Event-time over processing-time** \u2014 correct results even when events arrive out of order.
* **Separation of stream + batch** \u2014 the stream lands raw events fast; batch rollups and dbt build analytics, keeping each layer simple and replayable.

---

## \U0001F4DC License

MIT \u00a9 Aditya Yadav
