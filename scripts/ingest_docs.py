"""Ingest HTCondor Sphinx RST documentation into a FAISS vector index.

Usage:
    python scripts/ingest_docs.py [--output-dir data/htcondor_docs]

Clones a sparse copy of the htcondor/htcondor GitHub repo (docs/ only),
chunks the RST files by section, embeds with sentence-transformers, and
writes a FAISS index + JSON metadata to the output directory.

Re-run any time you want to refresh the index from the latest docs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TypedDict

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HTCONDOR_REPO = "https://github.com/htcondor/htcondor.git"
DOCS_SUBDIR = "docs"
EMBED_MODEL = "all-MiniLM-L6-v2"
MAX_CHUNK_TOKENS = 512
OVERLAP_TOKENS = 64
# Rough chars-per-token estimate (avoids a full tokenizer dependency)
CHARS_PER_TOKEN = 4

# RST underline characters that mark section headers
_RST_UNDERLINE_RE = re.compile(r"^([=\-~^\"'`#*+<>]{3,})\s*$")

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class Chunk(TypedDict):
    id: int
    source: str       # relative path inside docs/
    section: str      # section title (empty string for pre-title content)
    text: str


# ---------------------------------------------------------------------------
# Cloning
# ---------------------------------------------------------------------------


def sparse_clone_docs(target_dir: Path) -> Path:
    """Sparse-clone only the docs/ subtree of the HTCondor repo."""
    print(f"Cloning {HTCONDOR_REPO} (sparse, docs/ only) …")
    subprocess.run(
        [
            "git", "clone",
            "--depth", "1",
            "--filter=blob:none",
            "--sparse",
            HTCONDOR_REPO,
            str(target_dir),
        ],
        check=True,
    )
    subprocess.run(
        ["git", "sparse-checkout", "set", DOCS_SUBDIR],
        cwd=target_dir,
        check=True,
    )
    docs_path = target_dir / DOCS_SUBDIR
    if not docs_path.is_dir():
        sys.exit(f"ERROR: docs/ directory not found after clone: {docs_path}")
    return docs_path


# ---------------------------------------------------------------------------
# RST parsing
# ---------------------------------------------------------------------------


def _is_underline(line: str, prev_line: str) -> bool:
    """Return True if `line` is an RST section-header underline for `prev_line`."""
    m = _RST_UNDERLINE_RE.match(line)
    if not m:
        return False
    return len(line.rstrip()) >= len(prev_line.rstrip()) and len(prev_line.strip()) > 0


def split_rst_into_sections(text: str) -> list[tuple[str, str]]:
    """Split RST source into (title, body) pairs by section headers.

    Returns a list of (section_title, section_text) tuples.  The first
    item may have an empty title if there is content before the first header.
    """
    lines = text.splitlines(keepends=True)
    sections: list[tuple[str, str]] = []
    current_title = ""
    current_lines: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip("\n")
        if i > 0 and _is_underline(stripped, lines[i - 1].rstrip("\n")):
            # The previous line is the section title
            title_line = current_lines.pop() if current_lines else ""
            # Save the accumulated content before this section
            body = "".join(current_lines)
            if body.strip() or sections:
                sections.append((current_title, body))
            current_title = title_line.strip()
            current_lines = []
        else:
            current_lines.append(line)
        i += 1

    # Flush last section
    body = "".join(current_lines)
    if body.strip() or not sections:
        sections.append((current_title, body))

    return sections


def _char_chunks(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    """Split text into overlapping character-count chunks."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = end - overlap_chars
    return chunks


def chunk_rst_file(rst_path: Path, docs_root: Path) -> list[Chunk]:
    """Parse a single RST file into a list of Chunk dicts."""
    rel_path = str(rst_path.relative_to(docs_root))
    try:
        text = rst_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    sections = split_rst_into_sections(text)
    max_chars = MAX_CHUNK_TOKENS * CHARS_PER_TOKEN
    overlap_chars = OVERLAP_TOKENS * CHARS_PER_TOKEN

    chunks: list[Chunk] = []
    chunk_id = 0  # will be re-numbered by caller

    for title, body in sections:
        body = body.strip()
        if not body:
            continue
        if len(body) <= max_chars:
            chunks.append(Chunk(id=chunk_id, source=rel_path, section=title, text=body))
            chunk_id += 1
        else:
            for sub in _char_chunks(body, max_chars, overlap_chars):
                sub = sub.strip()
                if sub:
                    chunks.append(Chunk(id=chunk_id, source=rel_path, section=title, text=sub))
                    chunk_id += 1

    return chunks


def collect_all_chunks(docs_root: Path) -> list[Chunk]:
    """Walk the docs directory and return all chunks across all RST files."""
    rst_files = sorted(docs_root.rglob("*.rst"))
    print(f"Found {len(rst_files)} RST files.")
    all_chunks: list[Chunk] = []
    for rst in rst_files:
        file_chunks = chunk_rst_file(rst, docs_root)
        # Re-assign global IDs
        for c in file_chunks:
            c["id"] = len(all_chunks)
            all_chunks.append(c)
    print(f"Produced {len(all_chunks)} chunks.")
    return all_chunks


# ---------------------------------------------------------------------------
# Embedding & indexing
# ---------------------------------------------------------------------------


def embed_chunks(chunks: list[Chunk]) -> np.ndarray:
    """Embed all chunks with sentence-transformers; return float32 array."""
    print(f"Loading embedding model '{EMBED_MODEL}' …")
    model = SentenceTransformer(EMBED_MODEL)
    texts = [f"{c['section']}\n\n{c['text']}" if c["section"] else c["text"]
             for c in chunks]
    print(f"Embedding {len(texts)} chunks …")
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,  # cosine sim via inner product
    )
    return embeddings.astype("float32")


def build_faiss_index(embeddings: np.ndarray) -> faiss.Index:
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)  # inner product on L2-normalised vecs = cosine
    index.add(embeddings)
    return index


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest HTCondor docs into FAISS.")
    parser.add_argument(
        "--output-dir",
        default="data/htcondor_docs",
        help="Directory to write index.faiss and chunks.json (default: data/htcondor_docs)",
    )
    parser.add_argument(
        "--clone-dir",
        default=None,
        help="Use an existing clone instead of downloading (must contain docs/ subdir).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.clone_dir:
        docs_root = Path(args.clone_dir) / DOCS_SUBDIR
        if not docs_root.is_dir():
            sys.exit(f"ERROR: {docs_root} not found")
        chunks = collect_all_chunks(docs_root)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            docs_root = sparse_clone_docs(Path(tmp))
            chunks = collect_all_chunks(docs_root)

    if not chunks:
        sys.exit("ERROR: no chunks produced — check docs directory")

    embeddings = embed_chunks(chunks)
    index = build_faiss_index(embeddings)

    index_path = output_dir / "index.faiss"
    chunks_path = output_dir / "chunks.json"

    faiss.write_index(index, str(index_path))
    chunks_path.write_text(json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nWrote FAISS index  → {index_path}  ({os.path.getsize(index_path) // 1024} KB)")
    print(f"Wrote chunk metadata → {chunks_path}  ({len(chunks)} chunks)")


if __name__ == "__main__":
    main()
