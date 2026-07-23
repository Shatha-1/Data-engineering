# StreamForge — A Streaming Data Engineering Pipeline for AI Systems

**Student:** Waad Alsaif, Razan Almutairi, Dana Alsaidan, Layan Alameri, Shatha Bin Mana  
**Program:** Data Engineering  
**Session dates:** 19-7-2026 – 23-7-2026  
**Trainer:** Mohammed Albalawi
---

## Project Overview

StreamForge is an end-to-end data engineering pipeline that carries raw library
checkout events from a streaming source through to validated analytics tables and a
grounded, citation-backed question-answering layer.

The source data follows the shape used by most library circulation systems: one row
per checkout, with a checkout identifier, item format, a checkout and return
timestamp, the branch an item was checked out from and returned to, and a patron
category. Real circulation exports in this format are never fully clean — a
meaningful share of rows are still missing a return branch because the item had not
been scanned back in when the extract ran, a handful of checkouts are logged with a
return time earlier than the checkout time due to clock skew, and some loans last
only a few seconds because staff re-scanned an item at the counter instead of
completing the checkout.

Loaded without a validation boundary, those rows quietly distort every loan-duration
and demand figure computed downstream, and nobody notices until a dashboard number
looks wrong months later. StreamForge solves that by enforcing a machine-checked
contract at the point of ingestion and a hard quality gate in front of the analytics
and question-answering layers, so invalid data is stopped, logged with a reason, and
replayable — never silently absorbed into the lakehouse.

**Scope:** Kafka-based streaming ingestion with Pydantic schema validation, a
three-layer Delta lakehouse with an incremental `MERGE` upsert, a hybrid dense +
keyword RAG pipeline with citation tracing, Airflow orchestration, and per-stage
data-quality gating and OpenLineage lineage.

---

## Pipeline Architecture

```
Kafka producer (source export + correction batch)
        │
        ▼
Kafka consumer + LibraryCheckoutContract ──► quarantine CSV + DLQ topic
        │ (contract-valid records only)
        ▼
Bronze  (Delta, append-only, partitioned by checkout_branch_id)
        │
        ▼
Silver  (Delta MERGE upsert on checkout_id)
        │
        ▼
Quality Gate (Great Expectations) ──► raises QualityGateFailed on failure, halts pipeline
        │
        ├──────────────► Gold  (Delta aggregate: branch × month)
        └──────────────► RAG pipeline (hybrid retrieval + cited generation)
```

Every stage emits an OpenLineage `START` / `COMPLETE` / `FAIL` event under the
`streamforge` namespace.

### 1. Data Ingestion

A Kafka ingestion path built on `kafka-python`.

- **Producer** streams checkout rows into `library_checkouts_raw` as JSON, reading
  the bundled source file as raw strings so nothing is coerced before the contract
  sees it. The source data lives at `data/source/library_checkouts.csv` and ships
  with the repository — no download step or external credentials required.
- It then publishes a **correction batch** — a resample of already-sent checkouts
  republished with a filled-in `return_branch_id`, standing in for the nightly
  reconciliation feed a live circulation system would send once an item's check-in
  scan is finally recorded. This is what gives the Silver `MERGE` real matched keys
  to update, rather than a table built entirely from inserts.
- **Consumer** validates every message against `LibraryCheckoutContract`: a
  well-formed `checkout_id`, a recognised `item_format`, a known `patron_type`
  category, a non-blank `checkout_branch_id`, and — once a return exists — a loan
  window where `returned_at` follows `checked_out_at` by between 2 minutes and 21
  days.
- **Accepted** records go to a JSONL landing zone enriched with `kafka_offset` and
  `ingested_at`.
- **Rejected** records go to `quarantine_zone/` as CSV carrying the exact
  `rejection_reason`, and are republished to the `library_checkouts_dlq` dead-letter
  topic so the producing side can fix and replay them.

Nothing that fails the contract ever reaches Bronze.

### 2. Delta Lakehouse

Bronze / Silver / Gold on `pyspark` + `delta-spark`.

**Bronze** — append-only, partitioned by `checkout_branch_id`. Bronze is never
edited in place; when a business rule changes, Silver is rebuilt from Bronze
instead of re-ingesting from source.

**Silver** — a real Delta `MERGE` keyed on `checkout_id`. Matched keys are updated
with the corrected return branch and duration, unmatched keys are inserted, in one
atomic transaction. The source is de-duplicated to one row per key first, keeping
the most recently ingested version, since `MERGE` fails on duplicate source keys.
Schema enforcement is demonstrated explicitly: a write carrying an undeclared
`promo_code` column is refused by Delta rather than silently widening the table.

**Gold** — a genuine aggregate, not a filtered copy of Silver. Grouped by
`checkout_branch_id × checkout_month`, producing `total_checkouts`,
`distinct_return_branches`, `avg_loan_duration_min`, `member_share`, and
`media_share` (the share of loans that were DVDs or audiobooks rather than print
books).

### 3. RAG Pipeline

| Step | Implementation |
| --- | --- |
| Chunking | Sentence-level, 3 sentences per chunk, 1 sentence overlap |
| Embeddings | `all-MiniLM-L6-v2` bi-encoder (Sentence Transformers) |
| Vector store | ChromaDB, cosine-space HNSW index |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion, `k = 60`, parameter-free |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Generation | Groq when `GROQ_API_KEY` is set, otherwise the cited context itself |

Dense retrieval finds paraphrases of a question; BM25 finds exact terms such as
`loan_duration_min` or `QualityGateFailed`. RRF merges the two ranked lists without
any weight to tune, then the cross-encoder scores each `(query, chunk)` pair jointly
for a precise top-3.

**Citations.** Context blocks are numbered `[Source 1] … [Source N]`, the prompt
requires every factual sentence to carry a citation, and each run prints the map
from every source number back to its `chunk_id` and parent `doc_id`, so any claim in
the answer is traceable to the exact chunk it came from.

### 4. Pipeline Orchestration

An Apache Airflow DAG (`streamforge_library_checkout_pipeline`, 7 tasks) wires every
stage together:

```
ingest_produce
      └─> ingest_consume_validate
                └─> load_bronze
                          └─> upsert_silver
                                    └─> quality_gate
                                              ├─> build_gold
                                              └─> run_rag
```

`build_gold` and `run_rag` sit downstream of `quality_gate` with the default
`all_success` trigger rule, so a failed gate leaves both **skipped** — nothing is
published or indexed from unvalidated data.

### 5. Data Quality and Lineage

**Quality gate.** A Great Expectations 1.x suite on Silver validating: unique and
non-null `checkout_id`, non-null `checkout_branch_id`, positive `loan_duration_min`,
a `patron_type` and `item_format` restricted to known categories, and — for loans
that have actually been returned — `returned_at` strictly after `checked_out_at`.
`run_quality_gate` raises `QualityGateFailed` when the suite does not fully pass.
The gate is load-bearing, not advisory.

**Lineage.** Every task wraps its work in the `pipeline_stage` context manager,
which emits a real `openlineage-python` `RunEvent`: `START` on entry, `COMPLETE` on a
clean exit, `FAIL` if the stage raises — then re-raises so the orchestrator still
sees the failure. Events are written to `lineage_events/` via the file transport;
swapping it for the HTTP transport ships identical events to a Marquez server.

---

## Technologies Used

Python · Apache Kafka (`kafka-python`) · PySpark · Delta Lake (`delta-spark`) ·
Apache Airflow · Great Expectations · OpenLineage · ChromaDB · Sentence
Transformers · BM25 (`rank-bm25`) · Pydantic v2 · Groq (optional)

---

## How to Run

### Prerequisites

- **Python 3.11 or 3.12 recommended.** Several dependencies here — PySpark,
  Delta Lake, ChromaDB, Sentence Transformers — publish wheels for mainstream
  Python releases first, so a brand-new interpreter version (such as 3.14) can
  lag behind on compatible builds and cause `pip install` failures that have
  nothing to do with this project's code. If `pip install -r requirements.txt`
  fails on your interpreter, the fastest fix is switching to 3.11 or 3.12 via
  `pyenv` or a fresh virtual environment rather than debugging package errors.
- **JDK 17** — required by both Spark and the Kafka broker
- **A running Kafka broker** on `localhost:9092`
- No external credentials or downloads are needed for the source data — the
  bundled export at `data/source/library_checkouts.csv` is read directly.

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

Start a broker on `localhost:9092` before running the ingestion stage (for example,
a local single-node KRaft broker).

### 3. Configure credentials (optional)

```bash
export GROQ_API_KEY=<your-key>        # optional — enables LLM answer generation
```

### 4. Run the pipeline

```bash
python -m src.main
```

To capture the run as evidence:

```bash
python -m src.main 2>&1 | tee docs/sample_run.log
```

### Running under Airflow

Symlink `dags/` into `$AIRFLOW_HOME/dags` (or copy the repository into
`$AIRFLOW_HOME`) so that `src` is importable, start the scheduler, and trigger
`streamforge_library_checkout_pipeline` manually — it is not scheduled.

### Expected output

A successful run produces, in order:

| Stage | What you should see |
| --- | --- |
| Ingestion | Source rows published plus a correction batch on top; a majority accepted, with the remainder rejected across a handful of distinct reasons, each with a printed count |
| Bronze | The accepted-record count appended to the Delta Bronze table |
| Silver | A `MERGE` operation logged with `numTargetRowsUpdated` / `numTargetRowsInserted`, a lower row count than raw input after de-duplication, and a schema-enforcement rejection message from Delta |
| Quality gate | All Great Expectations checks `PASSED`, `success=True` |
| Gold | A branch × month aggregate table, a few dozen rows |
| RAG | 12 documents chunked into sentence-level windows; per query: dense + BM25 candidates, RRF fusion, cross-encoder reranking, a cited answer, and a source-to-chunk traceability map |
| Lineage | A `START` and a matching `COMPLETE` (or `FAIL`) event per stage under namespace `streamforge` |

If the quality gate fails, `main.py` prints `PIPELINE HALTED at the quality gate`,
a `FAIL` lineage event is emitted, and the process exits with status `1` — Gold and
RAG never run.

---

## Repository Structure

```
├── src/
│   ├── ingestion/
│   │   ├── contracts.py            # Pydantic data contract + business key
│   │   ├── producer.py             # Kafka producer + correction batch
│   │   └── consumer.py             # Kafka consumer, quarantine + DLQ routing
│   │
│   ├── lakehouse/
│   │   ├── spark_session.py        # Shared Spark + Delta session, Bronze schema
│   │   ├── bronze.py               # Landing zone -> Delta Bronze
│   │   ├── silver.py               # MERGE upsert + schema-enforcement proof
│   │   └── gold.py                 # Branch x month aggregate
│   │
│   ├── rag/
│   │   ├── knowledge_base.py       # Corpus describing the pipeline itself
│   │   └── pipeline.py             # Chunking, Chroma, BM25, RRF, reranking, citations
│   │
│   ├── quality/
│   │   └── expectations.py         # Great Expectations suite + gate
│   │
│   ├── lineage/
│   │   └── emitter.py              # OpenLineage START / COMPLETE / FAIL
│   │
│   ├── config.py                   # Paths, topics, model names
│   ├── tasks.py                    # The seven stages as callable tasks
│   ├── main.py                     # Local end-to-end runner
│   └── __init__.py
│
├── data/
│   └── source/
│       └── library_checkouts.csv   # Bundled sample source export
│
├── dags/
│   └── pipeline_dag.py             # Airflow DAG
│
├── docs/
│   └── ARCHITECTURE.md             # Design rationale and component detail
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Notes and Assumptions

- The bundled dataset at `data/source/library_checkouts.csv` is a synthetic sample
  built to exercise every contract rule and quality check in the pipeline (missing
  return branches, clock-skew rows, scan-error durations, over-length loans, and
  unrecognised categories). Any CSV with the seven columns listed in `contracts.py`
  will work if `_load_source_dataframe` is pointed at a different file.
- Numeric figures in the "Expected output" table above are illustrative — actual
  counts depend on the random sample drawn for the correction batch.
- Program name, session dates, and trainer name at the top of this document are
  left as placeholders for submission-specific details.

---

## Training Attribution

Completed as part of **[Program / course name]**.

**Cohort / session dates:** [Insert dates]
**Trainer:** [Insert trainer name]
