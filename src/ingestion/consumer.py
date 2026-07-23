"""
Reads `library_checkouts_raw`, checks every message against
`LibraryCheckoutContract`, and routes it one of two ways. Nothing that
fails validation is ever allowed to reach the Bronze layer — that
boundary is the entire point of putting the contract here rather than
relying on downstream cleanup.
"""
import csv
import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from src import config
from src.ingestion.contracts import LibraryCheckoutContract

logger = logging.getLogger(__name__)


def _ensure_dirs() -> None:
    config.LANDING_DIR.mkdir(parents=True, exist_ok=True)
    config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


def _classify(raw_message: dict) -> tuple[bool, str | None]:
    try:
        LibraryCheckoutContract(**raw_message)
        return True, None
    except ValidationError as exc:
        # Keep the first error only — enough to explain the rejection
        # without dumping the full Pydantic error tree into a CSV cell.
        first_error = exc.errors()[0]
        field = ".".join(str(p) for p in first_error["loc"]) or "record"
        return False, f"{field}: {first_error['msg']}"


def run_consumer(max_messages: int | None = None) -> dict:
    _ensure_dirs()

    consumer = KafkaConsumer(
        config.TOPIC_RAW,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=15_000,
    )
    dlq_producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    accepted_count = 0
    rejected_count = 0
    reasons: dict[str, int] = {}

    with open(config.LANDING_FILE, "w", encoding="utf-8") as landing_fh, \
         open(config.QUARANTINE_DIR / "rejected_checkouts.csv", "w", newline="", encoding="utf-8") as quarantine_fh:

        quarantine_writer = csv.writer(quarantine_fh)
        quarantine_writer.writerow(
            ["checkout_id", "item_format", "checked_out_at", "returned_at",
             "checkout_branch_id", "return_branch_id", "patron_type",
             "rejection_reason", "kafka_offset", "quarantined_at"]
        )

        for i, message in enumerate(consumer):
            if max_messages is not None and i >= max_messages:
                break

            raw = message.value
            is_valid, reason = _classify(raw)
            offset = message.offset

            if is_valid:
                enriched = dict(raw)
                enriched["kafka_offset"] = offset
                enriched["ingested_at"] = datetime.now(timezone.utc).isoformat()
                landing_fh.write(json.dumps(enriched) + "\n")
                accepted_count += 1
            else:
                quarantine_writer.writerow([
                    raw.get("checkout_id", ""), raw.get("item_format", ""),
                    raw.get("checked_out_at", ""), raw.get("returned_at", ""),
                    raw.get("checkout_branch_id", ""), raw.get("return_branch_id", ""),
                    raw.get("patron_type", ""), reason, offset,
                    datetime.now(timezone.utc).isoformat(),
                ])
                dlq_producer.send(config.TOPIC_DLQ, value=raw)
                rejected_count += 1
                key = reason.split(":")[0] if reason else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

    dlq_producer.flush()
    dlq_producer.close()
    consumer.close()

    summary = {
        "accepted": accepted_count,
        "rejected": rejected_count,
        "rejection_reasons": reasons,
        "landing_file": str(config.LANDING_FILE),
        "quarantine_file": str(config.QUARANTINE_DIR / "rejected_checkouts.csv"),
    }
    logger.info("Consumer summary: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_consumer()
