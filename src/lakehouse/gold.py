"""
Silver -> Gold: a branch-level, month-level rollup, rebuilt from scratch
on every run. Gold overwrites rather than merges because Silver is the
source of truth and aggregates are cheap enough to recompute in full —
that also removes any chance of drift between the two layers.
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config

logger = logging.getLogger(__name__)


def build_gold(spark: SparkSession) -> int:
    silver = spark.read.format("delta").load(config.SILVER_PATH)

    enriched = silver.withColumn("checkout_month", F.date_format("checked_out_at", "yyyy-MM"))

    gold = enriched.groupBy("checkout_branch_id", "checkout_month").agg(
        F.count("*").alias("total_checkouts"),
        F.countDistinct("return_branch_id").alias("distinct_return_branches"),
        F.round(F.avg("loan_duration_min"), 2).alias("avg_loan_duration_min"),
        F.round(
            F.avg(F.when(F.col("patron_type") == "member", 1).otherwise(0)), 3
        ).alias("member_share"),
        F.round(
            F.avg(F.when(F.col("item_format").isin("dvd", "audiobook"), 1).otherwise(0)), 3
        ).alias("media_share"),
    )

    gold.write.format("delta").mode("overwrite").save(config.GOLD_PATH)

    count = gold.count()
    logger.info("Gold: wrote %d branch-month rows to %s", count, config.GOLD_PATH)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.lakehouse.spark_session import get_spark_session

    spark_session = get_spark_session("streamforge-gold")
    build_gold(spark_session)
    spark_session.stop()
