# StreamForge — A Streaming Data Engineering Pipeline for AI Systems

**Trainees:** Waad Alsaif, Razan Almutairi, Dana Alsaidan, Layan Alameri, Shatha Bin Mana  
**Program:** Modern Data Engineering for AI Systems  
**Cohort Dates:** 19-07-2026 – 23-07-2026  
**Trainer:** Mohammed Albeladi  
**Academy:** Built as part of [SDAIA Academy](https://github.com/SDAIAAcademy)

---

## Project Overview

**StreamForge** is an end-to-end data engineering pipeline designed to ingest, process, validate, and serve library checkout events. It enforces strict data contracts at ingestion to catch malformed circulation records, processes data through a three-layer Delta Lakehouse, gates pipeline execution based on quality checks, and powers a cited hybrid RAG system.

**Scope:** Kafka streaming with Pydantic validation, 3-tier Delta Lakehouse (`MERGE` upserts), hybrid dense + BM25 RAG with reranking, Airflow orchestration, Great Expectations quality gate, and OpenLineage tracking.

---

## Architecture & Pipeline Overview
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

### Key Components:
1. **Ingestion (Kafka + Pydantic):** Ingests streaming records, validates via `LibraryCheckoutContract`. Valid records proceed; malformed records are routed to a local quarantine CSV and republished to a Dead-Letter Queue (DLQ) topic.
2. **Delta Lakehouse (PySpark + Delta):**
   * **Bronze:** Append-only raw data partitioned by branch.
   * **Silver:** Atomic Delta `MERGE` (upsert) based on `checkout_id` with schema enforcement proof.
   * **Gold:** Genuine business aggregation (branch × month metrics).
3. **Quality Gate & Lineage:** Great Expectations suite on Silver table. Failed quality gate halts downstream execution (`Gold` & `RAG`). OpenLineage emits `START`, `COMPLETE`, and `FAIL` events for every stage.
4. **RAG Pipeline:** Sentence-level chunking, ChromaDB dense vector search + BM25 keyword search fused via Reciprocal Rank Fusion (RRF), Cross-Encoder reranking, and citation-backed generation via Groq.
5. **Orchestration:** Apache Airflow DAG managing full dependency pipeline execution.

---

## Technologies Used

* **Streaming & Ingestion:** Apache Kafka (`kafka-python`), Pydantic v2
* **Storage & Processing:** PySpark, Delta Lake (`delta-spark`)
* **Orchestration & Governance:** Apache Airflow, Great Expectations 1.x, OpenLineage
* **Vector Store & RAG:** ChromaDB, Sentence Transformers, BM25 (`rank-bm25`), Groq API

---

## Setup & How to Run

### Prerequisites
* Python 3.11 or 3.12
* Java JDK 17
* A running Apache Kafka broker on `localhost:9092`

### 1. Installation
```bash
git clone <repo-url>
cd streamforge
pip install -r requirements.txt

