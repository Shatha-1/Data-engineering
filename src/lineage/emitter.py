"""
A thin wrapper around `openlineage-python` that every pipeline stage uses
to announce when it starts, finishes cleanly, or fails.

Events are written through the file transport to `lineage_events/`.
Pointing `_build_client` at the HTTP transport instead ships the same
events to a Marquez (or any OpenLineage-compatible) server without
touching any of the call sites in `src/tasks.py`.
"""
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport

from src import config

logger = logging.getLogger(__name__)


def _build_client() -> OpenLineageClient:
    config.LINEAGE_LOG_DIR.mkdir(parents=True, exist_ok=True)
    transport = FileTransport(
        FileConfig(log_file_path=str(config.LINEAGE_LOG_FILE), append=True)
    )
    return OpenLineageClient(transport=transport)


def _emit(client: OpenLineageClient, run_id: str, job_name: str, state: RunState) -> None:
    event = RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=config.LINEAGE_NAMESPACE, name=job_name),
        producer="https://github.com/streamforge/pipeline",
    )
    client.emit(event)
    logger.info("Lineage: %s %s (run_id=%s)", job_name, state.name, run_id)


@contextmanager
def pipeline_stage(job_name: str):
    """
    Wraps one pipeline stage: emits START on entry, COMPLETE on a clean
    exit, and FAIL (before re-raising) if the stage raises an exception —
    so the orchestrator still observes the failure while lineage still
    gets a record of it.
    """
    client = _build_client()
    run_id = str(uuid.uuid4())
    _emit(client, run_id, job_name, RunState.START)
    try:
        yield run_id
    except Exception:
        _emit(client, run_id, job_name, RunState.FAIL)
        raise
    else:
        _emit(client, run_id, job_name, RunState.COMPLETE)
