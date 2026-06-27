"""Spark Structured Streaming consumer.

Reads JSON events from Kafka, applies schema, watermarks for late data,
deduplicates by event_id, and writes to Delta with exactly-once semantics
via checkpointing.
"""

from __future__ import annotations
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

sys.path.append(".")
from src.config import (
    spark_session, KAFKA_BOOTSTRAP, KAFKA_TOPIC,
    EVENTS_RAW, CHECKPOINTS, MAX_OFFSETS_PER_TRIGGER, WATERMARK_DELAY,
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("page", StringType()),
    StructField("amount", DoubleType()),
])


def main():
    spark = spark_session("stream-consumer")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.date_trunc("hour", "event_ts"))
        # late-data tolerance + dedupe within watermark window
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )

    query = (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINTS / "events_raw"))
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")
        .start(EVENTS_RAW)
    )

    print(f"🟢 streaming → {EVENTS_RAW} (checkpoint: {CHECKPOINTS / 'events_raw'})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
