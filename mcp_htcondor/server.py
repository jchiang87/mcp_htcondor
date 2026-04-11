"""FastMCP server that exposes HTCondor tools defined as smolagents Tools.

Each @mcp.tool() function is a thin wrapper that delegates to the
corresponding smolagents Tool.forward() method.  The smolagents Tool
instances hold all HTCondor logic and can also be used directly with any
smolagents agent.

Run the server:
    python -m mcp_htcondor.server          # stdio transport (default)
    mcp-htcondor                           # via installed entry-point
"""

from __future__ import annotations

from typing import Optional

from mcp.server.fastmcp import FastMCP

from .htcondor_tools import (
    ActOnJobsTool,
    GetHtcondorConfigTool,
    LocateScheddsTool,
    QueryJobHistoryTool,
    QueryJobsTool,
    ReadJobEventsTool,
    SubmitDagTool,
    SubmitJobTool,
)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "htcondor",
    description=(
        "HTCondor job scheduler tools.  Provides access to the HTCondor "
        "Python bindings for querying jobs, submitting workflows, managing "
        "job state, reading event logs, and inspecting configuration."
    ),
)

# ---------------------------------------------------------------------------
# Smolagents tool instances (hold the HTCondor logic)
# ---------------------------------------------------------------------------

_query_jobs = QueryJobsTool()
_query_history = QueryJobHistoryTool()
_submit_job = SubmitJobTool()
_submit_dag = SubmitDagTool()
_act_on_jobs = ActOnJobsTool()
_locate_schedds = LocateScheddsTool()
_read_events = ReadJobEventsTool()
_get_config = GetHtcondorConfigTool()

# ---------------------------------------------------------------------------
# MCP tool wrappers
# ---------------------------------------------------------------------------


@mcp.tool()
def query_jobs(
    constraint: Optional[str] = None,
    projection: Optional[list[str]] = None,
    schedd_name: Optional[str] = None,
) -> str:
    """Query the HTCondor scheduler for active (queued or running) jobs.

    Returns a JSON object with 'jobs' (list of ClassAds) and 'count'.

    Args:
        constraint: ClassAd expression to filter jobs.  Examples:
            'Owner == "alice"', 'JobStatus == 2' (running),
            'ClusterId == 12345'.  Defaults to 'True' (all jobs).
        projection: List of job attribute names to return, e.g.
            ["ClusterId", "ProcId", "JobStatus", "Owner", "Cmd"].
            If None, all attributes are returned.
        schedd_name: Name of the schedd daemon.  Uses the local schedd
            when not specified.
    """
    return _query_jobs.forward(
        constraint=constraint,
        projection=list(projection) if projection else None,
        schedd_name=schedd_name,
    )


@mcp.tool()
def query_job_history(
    constraint: Optional[str] = None,
    projection: Optional[list[str]] = None,
    match: Optional[int] = None,
    schedd_name: Optional[str] = None,
) -> str:
    """Query the HTCondor scheduler for historical (completed/removed) jobs.

    Returns a JSON object with 'jobs' (list of ClassAds) and 'count'.

    Args:
        constraint: ClassAd expression to filter history jobs.  Examples:
            'Owner == "alice"', 'JobStatus == 4' (completed),
            'ClusterId == 12345'.
        projection: List of job attribute names to return.  If None, all
            attributes are returned.
        match: Maximum number of history records to return.  Default is
            unlimited.
        schedd_name: Name of the schedd daemon.  Uses the local schedd
            when not specified.
    """
    return _query_history.forward(
        constraint=constraint,
        projection=list(projection) if projection else None,
        match=match,
        schedd_name=schedd_name,
    )


@mcp.tool()
def submit_job(
    submit_description: dict,
    schedd_name: Optional[str] = None,
) -> str:
    """Submit one or more jobs to the HTCondor scheduler.

    Returns a JSON object with the 'cluster_id' of the submitted job(s).

    Args:
        submit_description: HTCondor submit description as a JSON object
            of key=value pairs.  Example:
            {"executable": "/bin/echo", "arguments": "hello",
             "output": "job.out", "error": "job.err",
             "log": "job.log", "queue": "1"}.
        schedd_name: Name of the schedd daemon.  Uses the local schedd
            when not specified.
    """
    return _submit_job.forward(
        submit_description=submit_description,
        schedd_name=schedd_name,
    )


@mcp.tool()
def submit_dag(
    dag_file: str,
    submit_options: Optional[dict] = None,
    schedd_name: Optional[str] = None,
) -> str:
    """Submit a DAGMan workflow to the HTCondor scheduler.

    Returns a JSON object with the 'cluster_id' of the DAGMan job.

    Args:
        dag_file: Path to the DAG description file (.dag).
        submit_options: Optional DAGMan submit options as a JSON object,
            e.g. {"MaxIdle": "10", "MaxJobs": "100"}.
        schedd_name: Name of the schedd daemon.  Uses the local schedd
            when not specified.
    """
    return _submit_dag.forward(
        dag_file=dag_file,
        submit_options=submit_options,
        schedd_name=schedd_name,
    )


@mcp.tool()
def act_on_jobs(
    action: str,
    constraint: str,
    schedd_name: Optional[str] = None,
) -> str:
    """Perform an action on HTCondor jobs matching a ClassAd constraint.

    Returns a JSON summary of the action result.

    Args:
        action: Action to perform.  One of: 'Remove' (graceful),
            'RemoveX' (forceful), 'Hold', 'Release', 'Suspend',
            'Continue', 'Vacate', 'VacateFast'.
        constraint: ClassAd constraint identifying the target jobs.
            Examples: 'ClusterId == 12345', 'Owner == "alice"',
            'JobStatus == 5' (held jobs).
        schedd_name: Name of the schedd daemon.  Uses the local schedd
            when not specified.
    """
    return _act_on_jobs.forward(
        action=action,
        constraint=constraint,
        schedd_name=schedd_name,
    )


@mcp.tool()
def locate_schedds(
    schedd_name: Optional[str] = None,
    constraint: Optional[str] = None,
) -> str:
    """Locate available HTCondor schedulers (schedds) via the Collector.

    Returns a JSON object with a 'schedds' list of daemon ClassAds
    including Name, Machine, TotalRunningJobs, TotalIdleJobs, etc.

    Args:
        schedd_name: Locate a specific schedd by name.  If not specified,
            all available schedds are returned.
        constraint: ClassAd constraint to filter Collector results.
    """
    return _locate_schedds.forward(
        schedd_name=schedd_name,
        constraint=constraint,
    )


@mcp.tool()
def read_job_events(
    log_file: str,
    stop_after: Optional[float] = None,
) -> str:
    """Read events from an HTCondor job event log file.

    Returns a JSON object with an 'events' list.  Each entry includes the
    event type name, cluster/proc IDs, timestamp, and all other event
    attributes present in the log.

    Args:
        log_file: Path to the HTCondor job event log file.
        stop_after: Seconds to wait for new events.  0 = non-blocking
            (default), -1 = block until a new event arrives.
    """
    return _read_events.forward(
        log_file=log_file,
        stop_after=stop_after,
    )


@mcp.tool()
def get_htcondor_config(
    param_name: Optional[str] = None,
) -> str:
    """Read HTCondor configuration parameters.

    Returns a JSON object with the requested parameter(s).

    Args:
        param_name: Name of a specific parameter to read, e.g.
            'SCHEDD_HOST', 'COLLECTOR_HOST', 'MAX_JOBS_RUNNING'.
            If not specified, all parameters are returned.
    """
    return _get_config.forward(param_name=param_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run() -> None:
    """Run the HTCondor MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    run()
