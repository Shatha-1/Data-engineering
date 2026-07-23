"""
Landing JSONL -> Delta Bronze.

Bronze is append-only and partitioned by `checkout_branch_id`, the column
every downstream reader filters on. If a business rule changes later,
Silver gets rebuilt from Bronze — Bronze itself is never edited in place.
"""
import logging

from pyspark.sql import DataFrame, SparkSession

from src import config

logger = logging.getLogger(__name__)


def load_bronze(spark: SparkSession) -> int:
    df: DataFrame = spark.read.json(str(config.LANDING_FILE))

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy(config.BRONZE_PARTITION_COLUMN)
        .save(config.BRONZE_PATH)
    )

    count = df.count()
    logger.info("Bronze: appended %d records to %s", count, config.BRONZE_PATH)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.lakehouse.spark_session import get_spark_session

    spark_session = get_spark_session("streamforge-bronze")
    load_bronze(spark_session)
    spark_session.stop()
