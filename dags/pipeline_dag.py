"""
Airflow DAG for the StreamForge library-checkout pipeline. Each task calls the matching
function in `src/tasks.py`, so the DAG and the local `main.py` runner
always execute identical logic.

    ingest_produce
          |
          v
    ingest_consume_validate
          |
          v
    load_bronze
          |
          v
    upsert_silver
          |
          v
    quality_gate
        /    \
       v      v
  build_gold  run_rag

`build_gold` and `run_rag` both use the default `all_success` trigger
rule, so if `quality_gate` raises, both are skipped and nothing is
published from data that never passed validation.
"""
from datetime import datetime

from airflow.decorators import dag, task

default_args = {
    "owner": "streamforge",
    "retries": 1,
}


@dag(
    dag_id="streamforge_library_checkout_pipeline",
    schedule=None,  # triggered manually
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["capstone", "data-engineering", "rag"],
)
def streamforge_pipeline():
    @task
    def ingest_produce():
        from src.tasks import task_ingest_produce

        return task_ingest_produce()

    @task
    def ingest_consume_validate():
        from src.tasks import task_ingest_consume_validate

        return task_ingest_consume_validate()

    @task
    def load_bronze():
        from src.tasks import task_load_bronze

        return task_load_bronze()

    @task
    def upsert_silver():
        from src.tasks import task_upsert_silver

        return task_upsert_silver()

    @task
    def quality_gate():
        from src.tasks import task_quality_gate

        return task_quality_gate()

    @task
    def build_gold():
        from src.tasks import task_build_gold

        return task_build_gold()

    @task
    def run_rag():
        from src.tasks import task_run_rag

        return task_run_rag()

    gate = quality_gate()
    ingest_produce() >> ingest_consume_validate() >> load_bronze() >> upsert_silver() >> gate
    gate >> [build_gold(), run_rag()]


streamforge_pipeline()
