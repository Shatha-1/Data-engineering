# Architecture

This document explains what each stage of the StreamForge pipeline guarantees and
why it was built the way it was, rather than repeating what the README already
covers at a summary level.

---

## 1. Data flow at a glance

| Stage | Reads from | Writes to | Module |
|---|---|---|---|
| Ingest — produce | bundled circulation export | `library_checkouts_raw` (Kafka) | `src/ingestion/producer.py` |
| Ingest — consume & validate | `library_checkouts_raw` | landing JSONL, quarantine CSV, `library_checkouts_dlq` | `src/ingestion/consumer.py` |
| Bronze | landing JSONL | `data/delta/bronze_checkouts` | `src/lakehouse/bronze.py` |
| Silver | Bronze | `data/delta/silver_checkouts` | `src/lakehouse/silver.py` |
| Quality gate | Silver | pass → continue, fail → raise | `src/quality/expectations.py` |
| Gold | Silver | `data/delta/gold_branch_activity` | `src/lakehouse/gold.py` |
| RAG | in-repo knowledge base | cited answers | `src/rag/pipeline.py` |

Every task in `src/tasks.py` wraps its work in `pipeline_stage` (`src/lineage/emitter.py`),
so each of the seven stages above also produces a lineage event trail.

---

## 2. Ingestion — where the contract lives

The producer (`producer.py`) reads the bundled `data/source/library_checkouts.csv`
export, reads every column as a raw string, and publishes each row to
`library_checkouts_raw` untouched. Reading as strings matters: if the producer
coerced types before publishing, malformed values would already be "fixed" by the
time the consumer ever saw them, and the contract would have nothing left to catch.

After the main batch, the producer resamples a set of already-published checkouts
and republishes them with a filled-in `return_branch_id`, standing in for the
nightly reconciliation feed a live circulation system sends once an item's check-in
scan is finally recorded. This second wave is what gives the Silver `MERGE` real
matched keys to update — without it, every row Silver ever sees would be a
first-time insert.

The consumer (`consumer.py`) checks every message against `LibraryCheckoutContract`
(`ingestion/contracts.py`):

| Rule | Typically rejects |
|---|---|
| `checkout_id` matches a 16-character alphanumeric pattern | malformed or truncated identifiers |
| `item_format` is a known format | corrupted category values |
| `patron_type` is `member` or `guest` | blank or unrecognised patron category |
| `checkout_branch_id` is non-blank | rows with no known checkout location |
| `returned_at` after `checked_out_at` (when a return exists), duration between 2 minutes and 21 days | clock-skew rows, scan errors, loans that were never properly returned |

Routing is binary:

- **Accepted** rows are appended to `data/landing/accepted_checkouts.jsonl`, enriched
  with `kafka_offset` and `ingested_at`.
- **Rejected** rows are written to `quarantine_zone/rejected_checkouts.csv` with a
  `rejection_reason` column, and republished to `library_checkouts_dlq` so whichever
  upstream process is producing bad rows has something to act on.

Nothing that fails the contract reaches Bronze — that boundary is the entire design
decision this stage exists to enforce.

### Business key

`business_key()` in `contracts.py` returns `checkout_id` directly, since this feed
already carries a globally unique identifier per checkout. That is a simpler
derivation than a composite key would require, and it is kept as an explicit
function rather than inlined everywhere so the key logic has exactly one place to
change if a future source only exposes, say, an item identifier plus a checkout
timestamp.

---

## 3. Lakehouse

### Bronze — append-only, partitioned by `checkout_branch_id`

Records land exactly as the consumer wrote them, partitioned by
`checkout_branch_id` because every downstream question this pipeline answers —
branch-level demand, which branches lend the most media, where floating-collection
returns pile up — filters on branch first. Bronze is never edited in place; if a
business rule changes, Silver gets rebuilt from Bronze rather than the source being
re-ingested.

### Silver — MERGE upsert

`_build_silver_source` types the timestamp columns, derives `loan_duration_min`,
and collapses the Bronze read to one row per `checkout_id` using a window function
ordered by `ingested_at` descending — the correction record wins over the original
because it was ingested later. That de-duplication is required for a separate
reason too: Delta's `MERGE` raises an error outright if the same key appears twice
on the source side.

```python
silver_table.alias("target").merge(
    source.alias("updates"), "target.checkout_id = updates.checkout_id"
).whenMatchedUpdate(set={
    "return_branch_id": "updates.return_branch_id",
    "returned_at": "updates.returned_at",
    "loan_duration_min": "updates.loan_duration_min",
    "ingested_at": "updates.ingested_at",
}).whenNotMatchedInsertAll().execute()
```

Matched keys are updated, unmatched keys inserted, both inside one atomic
transaction. `numTargetRowsUpdated` and `numTargetRowsInserted`, read back from the
Delta transaction history, are the evidence that the update branch actually
executed rather than every row silently falling through to insert.

**Schema enforcement.** `demonstrate_schema_enforcement` appends a row carrying an
undeclared `promo_code` column. Delta refuses the write, and the first line of its
own exception message is captured as proof. Without this guarantee, one careless
upstream change is enough to silently widen a production table that other teams
depend on.

### Gold — a real aggregate, rebuilt in full each run

Grouped by `checkout_branch_id × checkout_month`, Gold computes `total_checkouts`,
`distinct_return_branches`, `avg_loan_duration_min`, `member_share`, and
`media_share`. It overwrites rather than merges: Silver is the source of truth,
recomputing the aggregate from scratch is cheap, and overwriting removes any
possibility of the two layers drifting apart over time.

---

## 4. RAG pipeline

| Step | Implementation |
|---|---|
| Chunking | sentence-level, 3 sentences per chunk, 1 sentence overlap |
| Embeddings | `all-MiniLM-L6-v2` bi-encoder |
| Vector store | ChromaDB, cosine-space HNSW index |
| Keyword search | `rank_bm25.BM25Okapi` |
| Fusion | Reciprocal Rank Fusion, `k = 60` |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| Generation | Groq when `GROQ_API_KEY` is set; otherwise the cited context itself |

The corpus (`rag/knowledge_base.py`) describes the pipeline's own components — the
contract, the MERGE logic, the quality gate, lineage — so the question-answering
layer doubles as documentation someone new to the codebase can query directly
instead of reading every module first.

Dense retrieval is good at matching a question to a chunk that expresses the same
idea in different words; BM25 is better at surfacing a chunk that names an exact
identifier — `loan_duration_min`, `QualityGateFailed` — that an embedding model can
under-rank. Reciprocal Rank Fusion combines the two ranked lists without any weight
to tune, since a chunk's fused score depends only on its rank position in each list.
The cross-encoder then scores the query and each surviving candidate jointly, which
is a strictly more accurate — if more expensive — comparison than scoring two
independently embedded vectors, and the top three scores become the final context.

**Citations.** Context blocks are numbered `[Source 1] … [Source N]` before being
placed in the prompt, the prompt requires every factual sentence in the answer to
carry one of those numbers, and each run prints the mapping from every source
number back to its `chunk_id` and parent `doc_id` — so any claim in the final answer
can be traced to the exact passage that produced it.

---

## 5. Quality gate

Built on Great Expectations' fluent API against a pandas view of Silver. The suite
checks: `checkout_id` unique and non-null, `checkout_branch_id` non-null,
`loan_duration_min` positive, `patron_type` and `item_format` restricted to their
known categories, and `returned_at` strictly after `checked_out_at` for loans that
have actually been returned — rows still checked out (`returned_at` is null) are
skipped rather than failed on that check. `run_quality_gate` raises
`QualityGateFailed` the moment the suite does not fully pass.

Because `build_gold` and `run_rag` both sit downstream of `quality_gate` in the
Airflow DAG under the default `all_success` trigger rule, a raised
`QualityGateFailed` leaves both tasks skipped. The gate is load-bearing, not
advisory — nothing gets published or indexed from data that failed validation.

---

## 6. Orchestration and lineage

```
ingest_produce
      └─> ingest_consume_validate
                └─> load_bronze
                          └─> upsert_silver
                                    └─> quality_gate
                                              ├─> build_gold
                                              └─> run_rag
```

Every Airflow task in `dags/pipeline_dag.py` calls the matching function in
`src/tasks.py`, and every one of those functions wraps its body in `pipeline_stage`
(`src/lineage/emitter.py`):

- a `START` event is emitted on entry,
- a `COMPLETE` event on a clean exit,
- a `FAIL` event if the stage raises — the exception is then re-raised so Airflow
  still marks the task as failed.

Events are genuine `openlineage-python` `RunEvent` objects under the `streamforge`
namespace, written through the file transport to `lineage_events/openlineage_run.log`.
Swapping the file transport for the HTTP transport in `emitter.py` is the only
change needed to ship the same events to a running Marquez server.

Each Spark-backed task in `src/tasks.py` creates and stops its own `SparkSession`
rather than sharing one across the run, so retrying a single stage never depends on
the session state left behind by another.

---

## 7. Design notes

**Why a landing JSONL between Kafka and Bronze, instead of writing straight to
Delta from the consumer?** It keeps the consumer free of any Spark dependency, so
ingestion starts fast and the Bronze load can be retried independently of the Kafka
read that produced its input. It also doubles as the untransformed landing zone the
medallion pattern expects to sit ahead of Bronze.

**Why does the loan-duration floor sit at two minutes specifically?** In practice, a
return logged within seconds or a minute of checkout is almost always a scan error
— staff re-scanning an item at the counter rather than a patron actually leaving
with it. Treating those rows as real completed loans would deflate average loan
duration and inflate checkout counts at busy service desks where mis-scans happen
most often.

**Why keep quarantine and DLQ separate instead of just one or the other?** The
quarantine CSV is for a human auditing rejection patterns locally; the DLQ topic is
for whatever automated replay process eventually reprocesses corrected rows.
Neither substitutes for the other — a CSV file is not something a downstream
consumer can subscribe to, and a Kafka topic is not something someone can casually
skim in a spreadsheet.

**Why does the source data ship as a bundled CSV instead of a download step?**
Earlier revisions of this pipeline pulled a trip export through `kagglehub` at
runtime, which meant the pipeline could not actually run without external
credentials and network access. Bundling `data/source/library_checkouts.csv`
directly in the repository — a synthetic export engineered to exercise every
contract rule and quality check — means `python -m src.main` works immediately
after installing dependencies, with nothing to configure and nothing that can fail
because of an expired API key or a rate-limited download.
