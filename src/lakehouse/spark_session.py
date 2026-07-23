"""
One shared way to build a Spark session with Delta Lake wired in, and the
explicit schema that Bronze writes are pinned to.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session(app_name: str = "streamforge") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:  # pragma: no cover - delta-spark not installed locally
        pass

    return builder.getOrCreate()


# Bronze is a faithful, append-only record of what the consumer accepted:
# the contract fields plus the two enrichment columns added at ingestion
# time. Nothing here is typed or transformed yet — that happens in Silver.
BRONZE_SCHEMA = StructType(
    [
        StructField("checkout_id", StringType(), nullable=False),
        StructField("item_format", StringType(), nullable=True),
        StructField("checked_out_at", StringType(), nullable=True),
        StructField("returned_at", StringType(), nullable=True),
        StructField("checkout_branch_id", StringType(), nullable=True),
        StructField("return_branch_id", StringType(), nullable=True),
        StructField("patron_type", StringType(), nullable=True),
        StructField("kafka_offset", StringType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)
