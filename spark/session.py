"""One SparkSession builder for the streaming job, the maintenance command and the tests.

Delta Lake (delta-spark 3.3, the line for Spark 3.5) is configured here, so every entry point
reads and writes the same table format. The Kafka connector is added only where it is needed.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def build_spark(
    app: str, *, master: str | None = None, kafka: bool = False, conf: dict | None = None
) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        # VACUUM refuses retention under 7 days unless this is off; keep the check on.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    )
    if master:
        builder = builder.master(master)
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    packages = [KAFKA_PKG] if kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
