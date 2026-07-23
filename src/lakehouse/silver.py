"""
Bronze -> Silver via a real Delta `MERGE`, plus a small demonstration that
Delta refuses writes carrying columns the table was never told about.
"""
import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import config

logger = logging.getLogger(__name__)


def _build_silver_source(spark: SparkSession) -> DataFrame:
    """
    Types the timestamp columns, derives `loan_duration_min`, and keeps
    exactly one row per `checkout_id` — the most recently ingested one.
    This de-duplication matters because a Delta `MERGE` raises an error if
    the same key appears twice on the source side, and it is precisely
    what lets a correction record win over the checkout it is replacing.
    """
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)

    typed = bronze.withColumn("checked_out_at", F.to_timestamp("checked_out_at")).withColumn(
        "returned_at", F.to_timestamp("returned_at")
    )
    typed = typed.withColumn(
        "loan_duration_min",
        (F.unix_timestamp("returned_at") - F.unix_timestamp("checked_out_at")) / 60.0,
    )

    window = Window.partitionBy("checkout_id").orderBy(F.col("ingested_at").desc())
    deduped = (
        typed.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    return deduped


def upsert_silver(spark: SparkSession) -> dict:
    source = _build_silver_source(spark)

    if not DeltaTable.isDeltaTable(spark, config.SILVER_PATH):
        source.write.format("delta").mode("overwrite").save(config.SILVER_PATH)
        logger.info("Silver: initial load, %d rows", source.count())
        return {"mode": "initial_load", "rows": source.count()}

    silver_table = DeltaTable.forPath(spark, config.SILVER_PATH)

    (
        silver_table.alias("target")
        .merge(source.alias("updates"), "target.checkout_id = updates.checkout_id")
        .whenMatchedUpdate(
            set={
                "return_branch_id": "updates.return_branch_id",
                "returned_at": "updates.returned_at",
                "loan_duration_min": "updates.loan_duration_min",
                "ingested_at": "updates.ingested_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = silver_table.history(1).select(
        "operationMetrics.numTargetRowsUpdated",
        "operationMetrics.numTargetRowsInserted",
    ).first()

    result = {
        "mode": "merge",
        "rows_updated": int(history["numTargetRowsUpdated"]),
        "rows_inserted": int(history["numTargetRowsInserted"]),
        "total_rows": silver_table.toDF().count(),
    }
    logger.info("Silver MERGE result: %s", result)
    return result


def demonstrate_schema_enforcement(spark: SparkSession) -> str:
    """
    Appends a row carrying an undeclared `promo_code` column and returns
    Delta's rejection message. This is the guardrail that stops a single
    misconfigured upstream job from silently widening a production table.
    """
    bad_row = spark.createDataFrame(
        [("TESTLOAN0000001A", "book", None, None, "BR-101", "BR-104", "member", 12.0, "SUMMER10")],
        schema=[
            "checkout_id", "item_format", "checked_out_at", "returned_at",
            "checkout_branch_id", "return_branch_id", "patron_type",
            "loan_duration_min", "promo_code",
        ],
    )
    try:
        bad_row.write.format("delta").mode("append").save(config.SILVER_PATH)
        return "Write unexpectedly succeeded — schema enforcement did not trigger."
    except Exception as exc:  # noqa: BLE001 - we want Delta's own message
        message = str(exc).splitlines()[0]
        logger.info("Schema enforcement rejected the write: %s", message)
        return message


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.lakehouse.spark_session import get_spark_session

    spark_session = get_spark_session("streamforge-silver")
    upsert_silver(spark_session)
    demonstrate_schema_enforcement(spark_session)
    spark_session.stop()
