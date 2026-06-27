"""Batch hourly rollup — aggregates raw streamed events into hourly KPIs."""

from __future__ import annotations
import sys
from pyspark.sql import functions as F

sys.path.append(".")
from src.config import spark_session, EVENTS_RAW, EVENTS_HOURLY


def main():
    spark = spark_session("hourly-rollup")
    raw = spark.read.format("delta").load(EVENTS_RAW)

    hourly = (
        raw.groupBy("event_hour", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("event_hour", "event_type")
    )

    (
        hourly.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_hour")
        .save(EVENTS_HOURLY)
    )
    print(f"✅ hourly rollup written → {EVENTS_HOURLY}")
    spark.stop()


if __name__ == "__main__":
    main()
