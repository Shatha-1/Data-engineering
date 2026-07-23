"""
Central configuration for the StreamForge pipeline.

Every path, topic name, and model identifier that more than one module
needs to agree on lives here, so nothing is hard-coded twice.
"""
from pathlib import Path

# ---------------------------------------------------------------------------
# Filesystem layout
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
SOURCE_DATA_FILE = DATA_DIR / "source" / "library_checkouts.csv"
LANDING_DIR = DATA_DIR / "landing"
QUARANTINE_DIR = PROJECT_ROOT / "quarantine_zone"
DELTA_DIR = DATA_DIR / "delta"

BRONZE_PATH = str(DELTA_DIR / "bronze_checkouts")
SILVER_PATH = str(DELTA_DIR / "silver_checkouts")
GOLD_PATH = str(DELTA_DIR / "gold_branch_activity")

LANDING_FILE = LANDING_DIR / "accepted_checkouts.jsonl"
LINEAGE_LOG_DIR = PROJECT_ROOT / "lineage_events"
LINEAGE_LOG_FILE = LINEAGE_LOG_DIR / "openlineage_run.log"

# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_RAW = "library_checkouts_raw"
TOPIC_DLQ = "library_checkouts_dlq"

# Size of the synthetic CDC batch: previously accepted checkouts
# re-published with a corrected return_branch_id / returned_at, giving the
# Silver MERGE real matched keys instead of a pure insert-only load.
CORRECTION_BATCH_SIZE = 90

# ---------------------------------------------------------------------------
# Lakehouse
# ---------------------------------------------------------------------------
BRONZE_PARTITION_COLUMN = "checkout_branch_id"

# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------
GE_SUITE_NAME = "silver_checkouts_suite"
GE_CHECKPOINT_NAME = "silver_checkouts_checkpoint"

# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------
LINEAGE_NAMESPACE = "streamforge"

# ---------------------------------------------------------------------------
# RAG
# ---------------------------------------------------------------------------
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
RERANK_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"
CHROMA_COLLECTION = "streamforge_knowledge_base"

CHUNK_SENTENCES = 3          # sentences per chunk
CHUNK_OVERLAP_SENTENCES = 1  # sentence overlap between consecutive chunks

RRF_K = 60                   # Reciprocal Rank Fusion smoothing constant
TOP_K_CANDIDATES = 10        # candidates pulled from each retriever before fusion
TOP_K_FINAL = 3              # chunks kept after cross-encoder reranking

GROQ_MODEL = "llama-3.1-8b-instant"
