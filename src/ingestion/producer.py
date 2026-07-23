"""
Publishes raw checkout rows to `library_checkouts_raw` exactly as they
exist in the source file — no coercion, no cleaning. Validation is
deliberately not this module's job: the consumer owns that, so the wire
format always reflects what the source system actually sent.
"""
import json
import logging
import random
from typing import Iterator

import pandas as pd
from kafka import KafkaProducer

from src import config

logger = logging.getLogger(__name__)


def _load_source_dataframe() -> pd.DataFrame:
    """
    Loads the bundled monthly circulation export from `data/source/`
    as strings, so every field still looks exactly the way the source
    system wrote it when it reaches the contract check. The file ships
    with the repository — no external download or credentials needed.
    """
    return pd.read_csv(config.SOURCE_DATA_FILE, dtype=str, keep_default_na=False)


def _row_to_message(row: pd.Series) -> dict:
    return {
        "checkout_id": row.get("checkout_id", ""),
        "item_format": row.get("item_format", ""),
        "checked_out_at": row.get("checked_out_at", ""),
        "returned_at": row.get("returned_at", ""),
        "checkout_branch_id": row.get("checkout_branch_id", ""),
        "return_branch_id": row.get("return_branch_id", ""),
        "patron_type": row.get("patron_type", ""),
    }


def _build_correction_batch(sent_messages: list[dict]) -> Iterator[dict]:
    """
    Simulates the nightly reconciliation feed: a sample of loans that were
    still missing their return branch at first extract (the item had not
    been scanned back in yet) are re-published now that the return is
    known. This is the change-data-capture scenario that gives the Silver
    `MERGE` real matched keys to update instead of a table built entirely
    from inserts.
    """
    candidates = [m for m in sent_messages if m["checkout_id"]]
    sample_size = min(config.CORRECTION_BATCH_SIZE, len(candidates))
    sample = random.sample(candidates, sample_size) if sample_size else []

    for original in sample:
        corrected = dict(original)
        if not corrected["return_branch_id"]:
            corrected["return_branch_id"] = f"BR-{random.randint(101, 144)}"
        yield corrected


def run_producer() -> dict:
    """Streams the source export, then the correction batch, into Kafka."""
    df = _load_source_dataframe()
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent_messages = []
    for _, row in df.iterrows():
        message = _row_to_message(row)
        producer.send(config.TOPIC_RAW, value=message)
        sent_messages.append(message)

    correction_count = 0
    for corrected in _build_correction_batch(sent_messages):
        producer.send(config.TOPIC_RAW, value=corrected)
        correction_count += 1

    producer.flush()
    producer.close()

    summary = {
        "source_rows": len(sent_messages),
        "correction_rows": correction_count,
        "total_published": len(sent_messages) + correction_count,
    }
    logger.info("Producer summary: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_producer()
