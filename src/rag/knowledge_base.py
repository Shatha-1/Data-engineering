"""
The corpus the RAG stage indexes. Rather than pointing it at an external
manual, the documents describe StreamForge's own components — the
question-answering layer can therefore be used as a live onboarding tool
for whoever inherits this pipeline next.
"""

DOCUMENTS: list[dict] = [
    {
        "doc_id": "doc-01",
        "title": "Why the contract sits at the Kafka boundary",
        "text": (
            "A data contract only protects a system if it is enforced at the earliest "
            "possible point. In StreamForge that point is the Kafka consumer, not the "
            "Bronze writer and not a nightly cleanup job. Every message read from "
            "library_checkouts_raw is checked against LibraryCheckoutContract before it "
            "is allowed to reach the landing zone. A record that fails even one rule is "
            "written to the quarantine CSV with its rejection reason and republished to "
            "the dead-letter topic, and it never reaches Bronze. Enforcing the contract "
            "downstream instead would mean bad rows sit in the lakehouse for however long "
            "it takes someone to notice, silently corrupting every aggregate computed on "
            "top of them."
        ),
    },
    {
        "doc_id": "doc-02",
        "title": "What gets rejected and why",
        "text": (
            "Five rules make up the checkout contract. A loan must carry a well-formed "
            "sixteen character checkout_id, a recognised item_format, a known patron_type "
            "category, a non-blank checkout_branch_id, and — once a return exists — a "
            "returned_at that falls after checked_out_at. Loan duration is also bounded: "
            "anything under two minutes is almost always a scan error where staff re-scanned "
            "the item at the counter, and anything over the library's twenty-one day loan "
            "period usually means the record does not represent a normal completed loan. "
            "Rows missing a return_branch_id are common in this feed because many items are "
            "still checked out when the extract runs; those rows are accepted with a blank "
            "return branch, and many of them come back clean in the next batch's reconciliation."
        ),
    },
    {
        "doc_id": "doc-03",
        "title": "The correction batch and why Silver needs it",
        "text": (
            "The producer does not just stream the source export once. After the main batch, "
            "it resamples a set of previously sent checkouts and republishes them with a "
            "filled-in return_branch_id, simulating the nightly reconciliation feed a real "
            "circulation system would send once an item's check-in scan is finally recorded. "
            "Without this second wave, every row arriving at Silver would be a first-time "
            "insert, and the MERGE step would never actually exercise its update path. With "
            "it, some checkout_id values now appear twice in the raw stream, and Silver has "
            "to decide which version wins."
        ),
    },
    {
        "doc_id": "doc-04",
        "title": "How the Silver MERGE resolves duplicate keys",
        "text": (
            "Delta's MERGE operation refuses to run if the same key appears twice on the "
            "source side of the join, so build_silver_source first collapses the Bronze read "
            "down to one row per checkout_id, keeping whichever version has the latest "
            "ingested_at timestamp. That deduplicated dataframe is then merged into the Silver "
            "table on checkout_id: rows whose key already exists get their return_branch_id, "
            "returned_at and loan_duration_min columns updated, and rows with a new key are "
            "inserted. Both branches execute in one atomic transaction, and the run logs "
            "numTargetRowsUpdated and numTargetRowsInserted pulled straight from the Delta "
            "transaction history as proof the update path actually fired."
        ),
    },
    {
        "doc_id": "doc-05",
        "title": "Why Bronze is partitioned by checkout_branch_id",
        "text": (
            "Bronze is written with partitionBy(checkout_branch_id) rather than left flat or "
            "partitioned by date. Every analytical question this pipeline is built to answer — "
            "branch-level demand, which branches lend the most media, where floating-collection "
            "returns pile up — filters on branch first. With several dozen distinct branches in "
            "a typical monthly export, the partition column has enough cardinality to let a "
            "filtered read skip most of the directory tree without producing so many tiny "
            "partitions that small file overhead outweighs the benefit."
        ),
    },
    {
        "doc_id": "doc-06",
        "title": "Schema enforcement as a guardrail, not a formality",
        "text": (
            "demonstrate_schema_enforcement appends a single row to the Silver table that "
            "carries an extra promo_code column the table was never told about. Delta Lake "
            "rejects the write outright rather than silently adding the column, and the "
            "pipeline captures the first line of that rejection as evidence. This matters "
            "because a lakehouse without schema enforcement is one careless upstream change "
            "away from a production table quietly growing columns nobody downstream expects, "
            "which tends to break dashboards and models in ways that are hard to trace back to "
            "their root cause."
        ),
    },
    {
        "doc_id": "doc-07",
        "title": "What the Gold layer actually aggregates",
        "text": (
            "Gold is a genuine rollup, not a filtered view of Silver. It groups every validated "
            "checkout by checkout_branch_id and checkout_month and computes total_checkouts, "
            "the count of distinct return branches reached from that origin, average loan "
            "duration in minutes, the share of loans taken out by members versus guests, and "
            "the share of loans that were DVDs or audiobooks rather than print books. A month "
            "with thousands of individual checkout records collapses into a handful of rows "
            "per branch, and the table is fully overwritten on every run since Silver is always "
            "the source of truth."
        ),
    },
    {
        "doc_id": "doc-08",
        "title": "Why the quality gate can halt the whole pipeline",
        "text": (
            "The quality gate runs a Great Expectations suite against Silver, checking that "
            "checkout_id is unique and never null, that checkout_branch_id is always present, "
            "that loan_duration_min is positive, that patron_type and item_format only contain "
            "recognised categories, and that every returned_at falls after its checked_out_at "
            "for loans that have actually been returned. run_quality_gate raises "
            "QualityGateFailed the moment any of those checks does not pass. Because both the "
            "Gold build and the RAG indexing step depend on the gate succeeding, a single "
            "failed expectation is enough to stop both of them from running against data nobody "
            "has actually vetted."
        ),
    },
    {
        "doc_id": "doc-09",
        "title": "How lineage events trace a run",
        "text": (
            "Every task in the DAG wraps its work in the pipeline_stage context manager, which "
            "emits an OpenLineage RunEvent the moment the stage begins, another when it finishes "
            "without error, and a FAIL event — followed by re-raising the original exception — if "
            "the stage errors out. All events share a namespace of streamforge and are written "
            "through the file transport to lineage_events/openlineage_run.log. Because the "
            "transport is the only backend-specific piece of the emitter, swapping the file "
            "transport for the HTTP transport is enough to ship identical events to a running "
            "Marquez instance without touching any of the calling code in tasks.py."
        ),
    },
    {
        "doc_id": "doc-10",
        "title": "Retrieval design: why both dense and keyword search run",
        "text": (
            "The RAG pipeline runs two retrievers over the same chunk store and never relies on "
            "either alone. The dense retriever, built on the all-MiniLM-L6-v2 bi-encoder and "
            "served through a ChromaDB HNSW index, is good at matching a question to a chunk "
            "that expresses the same idea in different words. BM25Okapi, the keyword retriever, "
            "is better at pulling back a chunk that contains an exact identifier a question "
            "names directly, such as loan_duration_min or QualityGateFailed, which a "
            "paraphrase-oriented embedding model can sometimes rank lower than it should."
        ),
    },
    {
        "doc_id": "doc-11",
        "title": "Fusing and reranking the two candidate lists",
        "text": (
            "The dense and keyword candidate lists are combined with Reciprocal Rank Fusion "
            "using a smoothing constant of sixty, a method chosen specifically because it needs "
            "no weight tuned between the two retrievers — a chunk's fused score depends only on "
            "the rank position it holds in each list, not on the raw similarity or BM25 score. "
            "The top candidates that survive fusion are then passed through a cross-encoder "
            "reranker, cross-encoder/ms-marco-MiniLM-L-6-v2, which scores the query and each "
            "chunk jointly rather than as two separately embedded vectors, and the highest three "
            "scores become the context the generation step actually sees."
        ),
    },
    {
        "doc_id": "doc-12",
        "title": "Citations and generation",
        "text": (
            "Every retrieved chunk is numbered Source 1 through Source N before being placed in "
            "the generation prompt, and the prompt instructs the model that every factual "
            "sentence in its answer must reference one of those source numbers. When a "
            "GROQ_API_KEY is configured, an actual language model produces the final answer "
            "under that constraint; when it is not, the pipeline falls back to returning the "
            "numbered, cited context directly rather than generating anything unsupported. "
            "After each run the pipeline prints the mapping from every source number back to "
            "its chunk_id and parent doc_id, so any sentence in the final answer can be traced "
            "to the exact passage it came from."
        ),
    },
]
