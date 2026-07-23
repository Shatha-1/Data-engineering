"""
Chunking -> hybrid retrieval (dense + BM25) -> Reciprocal Rank Fusion ->
cross-encoder reranking -> cited generation.
"""
import logging
import os
import re
from dataclasses import dataclass

import chromadb
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder, SentenceTransformer

from src import config
from src.rag.knowledge_base import DOCUMENTS

logger = logging.getLogger(__name__)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


@dataclass
class Chunk:
    chunk_id: str
    doc_id: str
    title: str
    text: str


def _split_sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENTENCE_SPLIT.split(text.strip()) if s.strip()]


def build_chunks(documents: list[dict] = DOCUMENTS) -> list[Chunk]:
    """
    Sliding-window sentence chunking: `CHUNK_SENTENCES` sentences per chunk
    with `CHUNK_OVERLAP_SENTENCES` sentences shared between consecutive
    chunks, so a fact sitting near a chunk boundary is not orphaned from
    the sentence that gives it context.
    """
    chunks: list[Chunk] = []
    step = config.CHUNK_SENTENCES - config.CHUNK_OVERLAP_SENTENCES

    for doc in documents:
        sentences = _split_sentences(doc["text"])
        if not sentences:
            continue
        idx = 0
        position = 0
        while idx < len(sentences):
            window = sentences[idx: idx + config.CHUNK_SENTENCES]
            chunk_text = " ".join(window)
            chunks.append(
                Chunk(
                    chunk_id=f"{doc['doc_id']}-c{position}",
                    doc_id=doc["doc_id"],
                    title=doc["title"],
                    text=chunk_text,
                )
            )
            position += 1
            idx += step
    logger.info("Chunking: %d documents -> %d chunks", len(documents), len(chunks))
    return chunks


class HybridIndex:
    """Owns the embedding model, the Chroma collection, and the BM25 index."""

    def __init__(self, chunks: list[Chunk]):
        self.chunks = chunks
        self._by_id = {c.chunk_id: c for c in chunks}

        self.embedder = SentenceTransformer(config.EMBEDDING_MODEL)
        self.reranker = CrossEncoder(config.RERANK_MODEL)

        client = chromadb.EphemeralClient()
        self.collection = client.get_or_create_collection(
            name=config.CHROMA_COLLECTION, metadata={"hnsw:space": "cosine"}
        )
        embeddings = self.embedder.encode([c.text for c in chunks]).tolist()
        self.collection.add(
            ids=[c.chunk_id for c in chunks],
            documents=[c.text for c in chunks],
            embeddings=embeddings,
        )

        tokenized = [c.text.lower().split() for c in chunks]
        self.bm25 = BM25Okapi(tokenized)

    def dense_search(self, query: str, top_k: int) -> list[str]:
        query_embedding = self.embedder.encode([query]).tolist()
        result = self.collection.query(query_embeddings=query_embedding, n_results=top_k)
        return result["ids"][0]

    def bm25_search(self, query: str, top_k: int) -> list[str]:
        scores = self.bm25.get_scores(query.lower().split())
        ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        return [self.chunks[i].chunk_id for i in ranked[:top_k]]

    def reciprocal_rank_fusion(self, ranked_lists: list[list[str]], k: int = config.RRF_K) -> list[str]:
        scores: dict[str, float] = {}
        for ranked in ranked_lists:
            for rank, chunk_id in enumerate(ranked):
                scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (k + rank + 1)
        return sorted(scores, key=scores.get, reverse=True)

    def rerank(self, query: str, candidate_ids: list[str], top_k: int) -> list[str]:
        pairs = [(query, self._by_id[cid].text) for cid in candidate_ids]
        scores = self.reranker.predict(pairs)
        ranked = sorted(zip(candidate_ids, scores), key=lambda x: x[1], reverse=True)
        return [cid for cid, _ in ranked[:top_k]]

    def retrieve(self, query: str) -> list[Chunk]:
        dense_ids = self.dense_search(query, config.TOP_K_CANDIDATES)
        bm25_ids = self.bm25_search(query, config.TOP_K_CANDIDATES)
        fused = self.reciprocal_rank_fusion([dense_ids, bm25_ids])
        top_fused = fused[: config.TOP_K_CANDIDATES]
        final_ids = self.rerank(query, top_fused, config.TOP_K_FINAL)
        return [self._by_id[cid] for cid in final_ids]


def _build_prompt(query: str, chunks: list[Chunk]) -> tuple[str, dict[int, Chunk]]:
    source_map: dict[int, Chunk] = {}
    context_blocks = []
    for i, chunk in enumerate(chunks, start=1):
        source_map[i] = chunk
        context_blocks.append(f"[Source {i}] {chunk.text}")

    context = "\n\n".join(context_blocks)
    prompt = (
        "Answer the question using only the numbered sources below. Every factual "
        "sentence in your answer must end with the bracketed source number it came "
        f"from, e.g. [Source 2].\n\n{context}\n\nQuestion: {query}\nAnswer:"
    )
    return prompt, source_map


def _generate(prompt: str, context: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return context  # fall back to the cited context itself
    from groq import Groq

    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model=config.GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content


def answer_query(index: HybridIndex, query: str) -> dict:
    chunks = index.retrieve(query)
    prompt, source_map = _build_prompt(query, chunks)
    context = "\n\n".join(f"[Source {i}] {c.text}" for i, c in source_map.items())
    answer = _generate(prompt, context)

    citation_trace = {
        i: {"chunk_id": c.chunk_id, "doc_id": c.doc_id, "title": c.title}
        for i, c in source_map.items()
    }
    return {"query": query, "answer": answer, "citation_trace": citation_trace}


DEFAULT_EVAL_QUERIES = [
    "Where in the pipeline is the checkout contract enforced, and why there?",
    "What happens to a Silver row when the quality gate does not pass?",
    "How does Reciprocal Rank Fusion combine the dense and BM25 results?",
    "Why is Gold rebuilt from scratch instead of merged?",
]


def run_rag_demo(queries: list[str] = DEFAULT_EVAL_QUERIES) -> list[dict]:
    chunks = build_chunks()
    index = HybridIndex(chunks)
    return [answer_query(index, q) for q in queries]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for result in run_rag_demo():
        print(result["query"])
        print(result["answer"])
        print(result["citation_trace"])
        print("-" * 60)
