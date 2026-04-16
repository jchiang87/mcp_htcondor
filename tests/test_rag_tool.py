"""Unit tests for mcp_htcondor.rag_tool."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import mcp_htcondor.rag_tool as rag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_singletons():
    rag._index = None
    rag._chunks = None
    rag._model = None


@pytest.fixture(autouse=True)
def reset_singletons():
    _reset_singletons()
    yield
    _reset_singletons()


def _make_faiss_index(n_results: int = 1, dim: int = 384):
    mock_index = MagicMock()
    scores = np.array([[0.9 - i * 0.1 for i in range(n_results)]], dtype="float32")
    ids = np.array([[i for i in range(n_results)]], dtype="int64")
    mock_index.search.return_value = (scores, ids)
    return mock_index


def _make_chunks(n: int = 3):
    return [
        {"source": f"doc{i}.rst", "section": f"Section {i}", "text": f"text {i}"}
        for i in range(n)
    ]


def _make_model(dim: int = 384):
    mock_model = MagicMock()
    mock_model.encode.return_value = np.ones((1, dim), dtype="float32")
    return mock_model


def _setup_index_dir(tmp_path, chunks, mock_index):
    """Write chunks.json and a dummy index.faiss so existence checks pass."""
    (tmp_path / "chunks.json").write_text(json.dumps(chunks), encoding="utf-8")
    (tmp_path / "index.faiss").write_bytes(b"")  # existence only; faiss.read_index is mocked
    return mock_index


# ---------------------------------------------------------------------------
# _data_dir()
# ---------------------------------------------------------------------------

class TestDataDir:
    def test_default_path(self):
        env = {k: v for k, v in os.environ.items() if k != "HTCONDOR_DOCS_DIR"}
        with patch.dict(os.environ, env, clear=True):
            result = rag._data_dir()
        assert result == rag._DEFAULT_DATA_DIR  # task 1

    def test_env_var_override(self, tmp_path):
        with patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}):
            result = rag._data_dir()
        assert result == tmp_path  # task 2


# ---------------------------------------------------------------------------
# _load()
# ---------------------------------------------------------------------------

class TestLoad:
    def test_missing_files_raises(self, tmp_path):
        with patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}):
            with pytest.raises(FileNotFoundError, match="Run 'python scripts/ingest_docs.py'"):
                rag._load()  # task 3

    def test_loads_from_disk(self, tmp_path):
        chunks = _make_chunks(2)
        mock_index = _make_faiss_index()
        mock_model = _make_model()
        _setup_index_dir(tmp_path, chunks, mock_index)

        with patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}), \
             patch("faiss.read_index", return_value=mock_index) as mock_read, \
             patch("mcp_htcondor.rag_tool._load_sentence_transformer", return_value=mock_model):
            index, loaded_chunks, model = rag._load()

        assert index is mock_index  # task 4
        assert loaded_chunks == chunks
        mock_read.assert_called_once()

    def test_singleton_caching(self, tmp_path):
        chunks = _make_chunks(2)
        mock_index = _make_faiss_index()
        mock_model = _make_model()
        _setup_index_dir(tmp_path, chunks, mock_index)

        with patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}), \
             patch("faiss.read_index", return_value=mock_index) as mock_read, \
             patch("mcp_htcondor.rag_tool._load_sentence_transformer", return_value=mock_model):
            rag._load()
            rag._load()  # second call

        assert mock_read.call_count == 1  # task 5


# ---------------------------------------------------------------------------
# SearchHTCondorDocsTool.forward()
# ---------------------------------------------------------------------------

class TestForward:
    def _tool(self):
        return rag.SearchHTCondorDocsTool()

    def _ctx(self, tmp_path, chunks, mock_index, mock_model):
        _setup_index_dir(tmp_path, chunks, mock_index)
        return (
            patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}),
            patch("faiss.read_index", return_value=mock_index),
            patch("mcp_htcondor.rag_tool._load_sentence_transformer", return_value=mock_model),
        )

    def test_empty_query(self):
        result = json.loads(self._tool().forward(""))
        assert "error" in result  # task 6

    def test_whitespace_query(self):
        result = json.loads(self._tool().forward("   "))
        assert "error" in result  # task 6

    def test_missing_index_returns_error_json(self, tmp_path):
        with patch.dict(os.environ, {"HTCONDOR_DOCS_DIR": str(tmp_path)}):
            result = json.loads(self._tool().forward("submit a job"))
        assert "error" in result  # task 7
        assert "ingest_docs" in result["error"]

    def test_happy_path(self, tmp_path):
        chunks = _make_chunks(3)
        mock_index = _make_faiss_index(n_results=2)
        mock_model = _make_model()
        p1, p2, p3 = self._ctx(tmp_path, chunks, mock_index, mock_model)

        with p1, p2, p3:
            result = json.loads(self._tool().forward("submit a job", top_k=2))

        assert result["query"] == "submit a job"  # task 8
        assert len(result["results"]) == 2
        for r in result["results"]:
            assert {"source", "section", "text", "score"} <= r.keys()

    @pytest.mark.parametrize("top_k,expected_k", [
        (0, 1),    # clamp low
        (-5, 1),   # clamp negative
        (25, 20),  # clamp high
        (None, 5), # default
        (3, 3),    # normal
    ])
    def test_top_k_clamping(self, tmp_path, top_k, expected_k):
        chunks = _make_chunks(20)
        mock_index = MagicMock()
        scores = np.array([[0.9] * expected_k], dtype="float32")
        ids = np.array([list(range(expected_k))], dtype="int64")
        mock_index.search.return_value = (scores, ids)
        mock_model = _make_model()
        p1, p2, p3 = self._ctx(tmp_path, chunks, mock_index, mock_model)

        with p1, p2, p3:
            self._tool().forward("query", top_k=top_k)

        _, call_k = mock_index.search.call_args[0]
        assert call_k == expected_k  # task 9

    def test_negative_faiss_ids_skipped(self, tmp_path):
        chunks = _make_chunks(3)
        mock_index = MagicMock()
        mock_index.search.return_value = (
            np.array([[0.9, 0.8, 0.7]], dtype="float32"),
            np.array([[0, -1, 2]], dtype="int64"),
        )
        mock_model = _make_model()
        p1, p2, p3 = self._ctx(tmp_path, chunks, mock_index, mock_model)

        with p1, p2, p3:
            result = json.loads(self._tool().forward("query", top_k=3))

        assert len(result["results"]) == 2  # task 10 — id=-1 skipped
