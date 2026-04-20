"""FastMCP server that exposes a smolagents CodeAgent as a single MCP tool.

The agent has access to all HTCondor tools and the slurm-mcp tools,
enabling natural-language queries about S3DF cluster job state.

Run:
    mcp-htcondor-agent      # via installed entry-point (stdio transport)
"""
from __future__ import annotations

import logging

import yaml
from dotenv import dotenv_values
from mcp import StdioServerParameters
from mcp.server.fastmcp import FastMCP
from smolagents import CodeAgent, MCPClient, OpenAIServerModel

from .htcondor_tools import (
    GetHtcondorConfigTool,
    GetLogPathTool,
    ListAvailableLogsTool,
    LocateScheddsTool,
    QueryJobHistoryTool,
    QueryJobsTool,
    ReadDaemonLogTool,
    ReadJobEventsTool,
)

logging.getLogger("smolagents").setLevel(logging.FATAL)

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _get_model() -> OpenAIServerModel:
    with open("/sdf/home/j/jchiang/.ai_api_keys") as f:
        api_key = yaml.safe_load(f)["us.anthropic.claude-sonnet-4-6"]
    return OpenAIServerModel(
        model_id="us.anthropic.claude-sonnet-4-6",
        api_base="https://ai-api.slac.stanford.edu",
        api_key=api_key,
    )


# ---------------------------------------------------------------------------
# Agent (lazy singleton)
# ---------------------------------------------------------------------------

_agent: CodeAgent | None = None


def _get_agent() -> CodeAgent:
    global _agent
    if _agent is not None:
        return _agent

    tools = [
        QueryJobsTool(),
        QueryJobHistoryTool(),
        LocateScheddsTool(),
        ReadJobEventsTool(),
        GetHtcondorConfigTool(),
        ListAvailableLogsTool(),
        GetLogPathTool(),
        ReadDaemonLogTool(),
    ]

    env = dotenv_values("/sdf/home/j/jchiang/.config/slurm-mcp/.env")
    env["FASTMCP_SHOW_SERVER_BANNER"] = "0"
    mcp_client = MCPClient(
        StdioServerParameters(command="slurm-mcp", env=env),
        structured_output=True,
    )
    tools.extend(mcp_client.get_tools())

    _agent = CodeAgent(
        model=_get_model(),
        tools=tools,
        additional_authorized_imports=["os", "glob", "pandas", "json"],
        name="s3df_cluster_inspector",
        description="Inspects the state of HTCondor and Slurm jobs running at S3DF.",
        instructions=(
            "Aggregate information on HTCondor jobs running on the various "
            "schedds, and Slurm jobs on the S3DF cluster. "
            "Answer concisely with relevant numbers and summaries."
        ),
        verbosity_level=0,
    )
    return _agent


# ---------------------------------------------------------------------------
# FastMCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "s3df-cluster-inspector",
    instructions=(
        "A CodeAgent that inspects HTCondor and Slurm job state at S3DF. "
        "Pass a natural-language query to run_query and receive a summary."
    ),
)


@mcp.tool()
def run_query(query: str) -> str:
    """Run a natural-language query against the S3DF cluster inspector agent.

    The agent has access to HTCondor tools (query jobs, view logs, locate
    schedds) and Slurm tools (list jobs, check GPU availability, read logs).
    It uses a code-first paradigm: the agent writes and executes Python
    snippets to aggregate and summarise results across both schedulers.

    Args:
        query: Natural-language question about cluster state, e.g.
            'How many jobs are running on each HTCondor schedd?',
            'Are there any held jobs for user jchiang?',
            'What GPUs are available on the Slurm cluster?'
    """
    try:
        result = _get_agent().run(query)
        return str(result)
    except Exception as exc:
        return f"Error: {exc}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Run the agent MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    run()
