"""
Spark Structured Streaming job for real-time fraud analytics.

Reads card transactions from a Redpanda (Kafka) topic and runs two streaming
queries:

  1. Bronze  - persists every raw transaction to Parquet (faithful landing
               zone, the training source for a downstream ML model).
  2. Gold    - tumbling 1-minute windowed aggregations with a 2-minute
               watermark: transaction counts, total/avg amount, and counts
               of each detected fraud pattern. Written to Parquet and echoed
               to the console as windows close.

Run (inside the `fraud` conda env, with Redpanda up):
    python spark/streaming_job.py

Stop with Ctrl+C.
"""

import os
import platform

# ---------------------------------------------------------------------------
# Windows Hadoop shim: Spark needs winutils.exe + hadoop.dll to write files
# on Windows. We point HADOOP_HOME at C:\hadoop (where those binaries live)
# before any Spark import touches Hadoop. No-op on Linux/Mac.
# ---------------------------------------------------------------------------
if platform.system() == "Windows":
    hadoop_home = r"C:\hadoop"
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    count, sum as _sum, avg, when,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

# ---------------------------------------------------------------------------
# Paths & config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "_checkpoints")

KAFKA_BOOTSTRAP = "localhost:19092"
TOPIC = "transactions"

# Kafka connector package (Spark downloads the JAR on first run).
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# Schema of the JSON payload produced by producer.py
SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("is_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("realtime-fraud-streaming")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ----- Source: read the stream from Redpanda --------------------------
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka value is bytes -> string -> parsed JSON columns.
    parsed = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # ----- Bronze: persist raw transactions -------------------------------
    bronze_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", BRONZE_DIR)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "bronze"))
        .outputMode("append")
        .queryName("bronze_raw")
        .start()
    )

    # ----- Gold: 1-minute windowed aggregations with watermark ------------
    gold = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(
            count("*").alias("tx_count"),
            _sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            _sum(when(col("fraud_type") == "high_amount", 1).otherwise(0)).alias("fraud_high_amount"),
            _sum(when(col("fraud_type") == "card_testing", 1).otherwise(0)).alias("fraud_card_testing"),
            _sum(when(col("fraud_type") == "impossible_travel", 1).otherwise(0)).alias("fraud_impossible_travel"),
            _sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_total"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "tx_count", "total_amount", "avg_amount",
            "fraud_high_amount", "fraud_card_testing",
            "fraud_impossible_travel", "fraud_total",
        )
    )

    # Write Gold to Parquet (append; complete mode can't write Parquet).
    gold_query = (
        gold.writeStream
        .format("parquet")
        .option("path", GOLD_DIR)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "gold"))
        .outputMode("append")
        .queryName("gold_windows")
        .start()
    )

    # Also echo closed windows to the console for live visibility.
    console_query = (
        gold.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .queryName("gold_console")
        .start()
    )

    print("Streaming started. Bronze + Gold running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
