"""Central configuration for the Real-Time Events Pipeline."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LAKEHOUSE = BASE_DIR / "lakehouse"
CHECKPOINTS = BASE_DIR / "checkpoints"

# Kafka
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "user_events"

# Delta sinks
EVENTS_RAW = str(LAKEHOUSE / "events_raw")
EVENTS_HOURLY = str(LAKEHOUSE / "events_hourly")

# Streaming tuning
MAX_OFFSETS_PER_TRIGGER = 5000     # backpressure
WATERMARK_DELAY = "10 minutes"     # late-data tolerance

EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "login", "logout"]


def spark_session(app_name: str = "realtime-events"):
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    extra = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
