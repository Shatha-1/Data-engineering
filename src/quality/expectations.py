"""
The gate between Silver and everything downstream of it (Gold and RAG).

Built on Great Expectations' fluent API against a pandas view of Silver.
The gate is not advisory: `run_quality_gate` raises `QualityGateFailed`
when the checkpoint does not pass, and the orchestrator treats that
exception as a hard stop.
"""
import logging

import great_expectations as gx
from pyspark.sql import SparkSession

from src import config

logger = logging.getLogger(__name__)


class QualityGateFailed(Exception):
    """Raised when the Silver checkpoint does not fully pass."""


def _build_suite(context: "gx.data_context.AbstractDataContext", batch):
    validator = context.get_validator(
        batch_request=batch, expectation_suite_name=config.GE_SUITE_NAME
    )

    validator.expect_column_values_to_be_unique("checkout_id")
    validator.expect_column_values_to_not_be_null("checkout_id")
    validator.expect_column_values_to_not_be_null("checkout_branch_id")
    validator.expect_column_values_to_be_between("loan_duration_min", min_value=1, strict_min=False)
    validator.expect_column_values_to_be_in_set("patron_type", ["member", "guest"])
    validator.expect_column_values_to_be_in_set(
        "item_format", ["book", "dvd", "audiobook"]
    )
    # Items still checked out have no returned_at yet — that is a valid
    # state, not a violation, so rows missing either side of the pair are
    # skipped rather than failed.
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        "returned_at", "checked_out_at", ignore_row_if="either_value_is_missing"
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    return validator


def run_quality_gate(spark: SparkSession) -> dict:
    silver_pdf = spark.read.format("delta").load(config.SILVER_PATH).toPandas()

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("silver_pandas")
    data_asset = data_source.add_dataframe_asset(name="silver_checkouts")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": silver_pdf})

    validator = _build_suite(context, batch)

    # `validator.validate()` runs the suite as a single-batch checkpoint under
    # the hood; the named checkpoint (config.GE_CHECKPOINT_NAME) is what a
    # scheduled run would call directly once the suite has stabilised.
    result = validator.validate()
    passed_count = sum(1 for r in result["results"] if r["success"])
    total_count = len(result["results"])

    summary = {
        "success": bool(result["success"]),
        "checks_passed": passed_count,
        "checks_total": total_count,
    }
    logger.info("Quality gate result: %s", summary)

    if not summary["success"]:
        raise QualityGateFailed(
            f"Silver failed {total_count - passed_count} of {total_count} expectations; "
            "Gold and RAG will not run."
        )
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.lakehouse.spark_session import get_spark_session

    spark_session = get_spark_session("streamforge-quality")
    run_quality_gate(spark_session)
    spark_session.stop()
