"""RAG tool for searching HTCondor documentation.

SearchHTCondorDocsTool is a smolagents Tool that performs semantic search
over a pre-built FAISS index of the HTCondor Sphinx RST documentation.

Build the index first:
    python scripts/ingest_docs.py

The index is expected at data/htcondor_docs/ relative to the project root
(i.e. the directory two levels above this file).  Override with the
HTCONDOR_DOCS_DIR environment variable.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import faiss
import numpy as np
from smolagents import Tool

# ---------------------------------------------------------------------------
# Index location
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent          # mcp_htcondor/
_PROJECT_ROOT = _HERE.parent           # project root
_DEFAULT_DATA_DIR = _PROJECT_ROOT / "data" / "htcondor_docs"


def _data_dir() -> Path:
    override = os.environ.get("HTCONDOR_DOCS_DIR")
    return Path(override) if override else _DEFAULT_DATA_DIR


# ---------------------------------------------------------------------------
# Lazy-loaded singletons
# ---------------------------------------------------------------------------

_index: faiss.Index | None = None
_chunks: list[dict[str, Any]] | None = None
_model: Any | None = None  # SentenceTransformer, imported lazily


def _load_sentence_transformer(model_name: str) -> Any:
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(model_name)


def _load() -> tuple[faiss.Index, list[dict[str, Any]], Any]:
    global _index, _chunks, _model
    if _index is None:
        data_dir = _data_dir()
        index_path = data_dir / "index.faiss"
        chunks_path = data_dir / "chunks.json"
        if not index_path.exists() or not chunks_path.exists():
            raise FileNotFoundError(
                f"HTCondor docs index not found at {data_dir}. "
                "Run 'python scripts/ingest_docs.py' first."
            )
        _index = faiss.read_index(str(index_path))
        _chunks = json.loads(chunks_path.read_text(encoding="utf-8"))

    if _model is None:
        _model = _load_sentence_transformer("all-MiniLM-L6-v2")

    return _index, _chunks, _model


# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------


class SearchHTCondorDocsTool(Tool):
    name = "search_htcondor_docs"
    description = (
        "Search the HTCondor documentation using semantic (vector) search. "
        "Returns the most relevant documentation excerpts for a given query. "
        "Use this to answer questions about HTCondor configuration, job submission, "
        "ClassAd expressions, daemons, APIs, and administrator/user procedures."
    )
    inputs = {
        "query": {
            "type": "string",
            "description": "Natural-language question or keyword query about HTCondor.",
        },
        "top_k": {
            "type": "integer",
            "description": "Number of results to return (default 5, max 20).",
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(self, query: str, top_k: int | None = 5) -> str:
        if not query or not query.strip():
            return json.dumps({"error": "query must not be empty"})

        k = max(1, min(int(top_k if top_k is not None else 5), 20))

        try:
            index, chunks, model = _load()
        except FileNotFoundError as exc:
            return json.dumps({"error": str(exc)})

        vec = model.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype("float32")

        scores, ids = index.search(vec, k)

        results = []
        for score, idx in zip(scores[0].tolist(), ids[0].tolist()):
            if idx < 0:
                continue
            chunk = chunks[idx]
            results.append(
                {
                    "source": chunk["source"],
                    "section": chunk["section"],
                    "text": chunk["text"],
                    "score": round(float(score), 4),
                }
            )

        return json.dumps({"query": query, "results": results}, ensure_ascii=False, indent=2)
