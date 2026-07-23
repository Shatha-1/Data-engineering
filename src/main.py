"""
Runs all seven stages locally, in order, and halts before Gold/RAG if the
quality gate raises.
"""
import logging
import sys

from src.quality.expectations import QualityGateFailed
from src.tasks import (
    task_build_gold,
    task_ingest_consume_validate,
    task_ingest_produce,
    task_load_bronze,
    task_quality_gate,
    task_run_rag,
    task_upsert_silver,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("streamforge.main")


def run_pipeline() -> None:
    logger.info("=== STAGE 1/7: ingest_produce ===")
    task_ingest_produce()

    logger.info("=== STAGE 2/7: ingest_consume_validate ===")
    task_ingest_consume_validate()

    logger.info("=== STAGE 3/7: load_bronze ===")
    task_load_bronze()

    logger.info("=== STAGE 4/7: upsert_silver ===")
    task_upsert_silver()

    logger.info("=== STAGE 5/7: quality_gate ===")
    try:
        task_quality_gate()
    except QualityGateFailed as exc:
        logger.error("PIPELINE HALTED at the quality gate: %s", exc)
        sys.exit(1)

    logger.info("=== STAGE 6/7: build_gold ===")
    task_build_gold()

    logger.info("=== STAGE 7/7: run_rag ===")
    task_run_rag()

    logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
