"""
The seven stages of the pipeline as plain callables, each wrapped in
`pipeline_stage` for lineage. `main.py` calls these directly for a local
run; `dags/pipeline_dag.py` calls the same functions from Airflow tasks,
so the two entry points can never drift apart.
"""
import logging

from src.ingestion.consumer import run_consumer
from src.ingestion.producer import run_producer
from src.lakehouse.bronze import load_bronze
from src.lakehouse.gold import build_gold
from src.lakehouse.silver import demonstrate_schema_enforcement, upsert_silver
from src.lakehouse.spark_session import get_spark_session
from src.lineage.emitter import pipeline_stage
from src.quality.expectations import run_quality_gate
from src.rag.pipeline import run_rag_demo

logger = logging.getLogger(__name__)


def task_ingest_produce() -> dict:
    with pipeline_stage("ingest_produce"):
        return run_producer()


def task_ingest_consume_validate() -> dict:
    with pipeline_stage("ingest_consume_validate"):
        return run_consumer()


def task_load_bronze() -> int:
    with pipeline_stage("load_bronze"):
        spark = get_spark_session("streamforge-bronze")
        try:
            return load_bronze(spark)
        finally:
            spark.stop()


def task_upsert_silver() -> dict:
    with pipeline_stage("upsert_silver"):
        spark = get_spark_session("streamforge-silver")
        try:
            merge_result = upsert_silver(spark)
            enforcement_message = demonstrate_schema_enforcement(spark)
            merge_result["schema_enforcement_message"] = enforcement_message
            return merge_result
        finally:
            spark.stop()


def task_quality_gate() -> dict:
    with pipeline_stage("quality_gate"):
        spark = get_spark_session("streamforge-quality")
        try:
            return run_quality_gate(spark)
        finally:
            spark.stop()


def task_build_gold() -> int:
    with pipeline_stage("build_gold"):
        spark = get_spark_session("streamforge-gold")
        try:
            return build_gold(spark)
        finally:
            spark.stop()


def task_run_rag() -> list[dict]:
    with pipeline_stage("run_rag"):
        return run_rag_demo()
