# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mcp-htcondor** is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes HTCondor job scheduler capabilities via the `smolagents` Tool interface. It enables Claude and other MCP clients to query job status, submit workflows, and manage HTCondor resources through natural language.

## Commands

```bash
# Install in development mode
pip install -e .

# Run the standard MCP server (stdio transport)
mcp-htcondor
# or: python -m mcp_htcondor.server

# Run the agent-based cluster inspector server
mcp-htcondor-agent

# Build the FAISS documentation index (required for RAG tool)
python scripts/ingest_docs.py

# Run tests
pytest tests/
pytest tests/test_rag_tool.py -v  # single test file
```

## Architecture

The project has two layers: **smolagents Tool classes** that wrap HTCondor Python bindings, and **FastMCP servers** that expose those tools via the MCP protocol.

### Core Layers

**`mcp_htcondor/htcondor_tools.py`** — 11 smolagents `Tool` subclasses, each wrapping one HTCondor operation (query jobs, submit, hold/release, read logs, etc.). Tools produce JSON string output and can be used standalone via `tool.forward(...)` or through an agent. Helper functions `_locate_schedd()`, `_ad_to_dict()`, and `_read_log_file_tail()` are shared utilities.

**`mcp_htcondor/server.py`** — FastMCP server named `"htcondor"`. Instantiates all 11 tools as module-level singletons and wraps each in a thin `@mcp.tool()` function that delegates to `tool.forward()`. Uses stdio transport.

**`mcp_htcondor/agent_server.py`** — FastMCP server exposing a single `run_query(query)` MCP tool. Internally runs a `smolagents` `CodeAgent` with all HTCondor tools plus Slurm tools loaded via `MCPClient`. The agent is lazily initialized as a singleton. Reads API credentials from `~/.ai_api_keys`. Uses `claude-sonnet-4-6` via a custom Anthropic-compatible API endpoint.

**`mcp_htcondor/rag_tool.py`** — `SearchHTCondorDocsTool` implements semantic search over HTCondor documentation. Uses `all-MiniLM-L6-v2` embeddings and a FAISS `IndexFlatIP` (cosine similarity). Index lives in `data/htcondor_docs/` (overridable via `HTCONDOR_DOCS_DIR` env var) and must be built with `scripts/ingest_docs.py` before use.

### Data Flow

```
MCP Client (Claude, IDE)
    ↓ MCP protocol (stdio)
FastMCP server (server.py or agent_server.py)
    ↓ tool.forward() calls
smolagents Tool classes (htcondor_tools.py)
    ↓ htcondor Python bindings
HTCondor daemons (schedd, collector, etc.)
```

For the agent server, a `CodeAgent` sits between FastMCP and the tools, allowing multi-step reasoning over HTCondor + Slurm state before returning a natural-language answer.

### Key Design Decisions

- **Singleton tool instances**: Tools are instantiated once at module level in `server.py` so any lazy-loaded state (models, connections) persists across calls.
- **JSON-only output**: All `Tool.forward()` methods return JSON strings so results are MCP-compatible and agent-parseable.
- **Dual-use tools**: Every tool works standalone (`tool.forward(...)`) and via the MCP server — see `examples/use_tools_directly.py` for direct usage patterns.
- **ClassAd serialization**: HTCondor `ClassAd` objects are converted to plain dicts via `_ad_to_dict()` before JSON serialization.

## Documentation Index

The RAG tool (`SearchHTCondorDocsTool`) requires a pre-built FAISS index. `scripts/ingest_docs.py` sparse-clones the `htcondor/htcondor` repo (docs only), parses RST files into sections, chunks them (512 tokens, 64 overlap), embeds with sentence-transformers, and writes `data/htcondor_docs/index.faiss` + `chunks.json`. The `data/` directory is gitignored.
