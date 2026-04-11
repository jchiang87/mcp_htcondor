"""Examples: using the HTCondor smolagents tools directly.

The Tool.forward() method can be called without any MCP or agent
machinery.  Each call returns a JSON string; use json.loads() to get
a Python dict back.

Run from the repo root (after setting up the environment):
    python examples/use_tools_directly.py
"""

import json
import sys

sys.path.insert(0, ".")  # make mcp_htcondor importable from the repo root

from mcp_htcondor import (
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
# Instantiate tools once; reuse them as needed
# ---------------------------------------------------------------------------

query_jobs       = QueryJobsTool()
query_history    = QueryJobHistoryTool()
submit_job       = SubmitJobTool()
submit_dag       = SubmitDagTool()
act_on_jobs      = ActOnJobsTool()
locate_schedds   = LocateScheddsTool()
read_job_events  = ReadJobEventsTool()
get_config       = GetHtcondorConfigTool()


# ---------------------------------------------------------------------------
# 1. List all available schedds
# ---------------------------------------------------------------------------

result = json.loads(locate_schedds.forward())
print(f"Found {result['count']} schedd(s):")
for s in result["schedds"]:
    print(f"  {s.get('Name')}  machine={s.get('Machine')}")


# ---------------------------------------------------------------------------
# 2. Query active jobs owned by the current user
# ---------------------------------------------------------------------------

import getpass
owner = getpass.getuser()

result = json.loads(query_jobs.forward(
    constraint=f'Owner == "{owner}"',
    projection=["ClusterId", "ProcId", "JobStatus", "Cmd", "Args"],
))
print(f"\nActive jobs for {owner}: {result['count']}")
for job in result["jobs"]:
    print(f"  {job['ClusterId']}.{job['ProcId']}  status={job.get('JobStatus')}  cmd={job.get('Cmd')}")


# ---------------------------------------------------------------------------
# 3. Query job history (last 5 completed jobs)
# ---------------------------------------------------------------------------

result = json.loads(query_history.forward(
    constraint=f'Owner == "{owner}"',
    projection=["ClusterId", "ProcId", "JobStatus", "ExitCode", "CompletionDate"],
    match=5,
))
print(f"\nLast {result['count']} history job(s) for {owner}:")
for job in result["jobs"]:
    print(f"  {job['ClusterId']}.{job['ProcId']}  exit={job.get('ExitCode')}  completed={job.get('CompletionDate')}")


# ---------------------------------------------------------------------------
# 4. Submit a simple job
# ---------------------------------------------------------------------------

result = json.loads(submit_job.forward(
    submit_description={
        "executable": "/bin/echo",
        "arguments":  "hello from mcp_htcondor",
        "output":     "/tmp/mcp_htcondor_test.out",
        "error":      "/tmp/mcp_htcondor_test.err",
        "log":        "/tmp/mcp_htcondor_test.log",
        "queue":      "1",
    }
))
if "error" in result:
    print(f"\nSubmit failed: {result['error']}")
else:
    cluster_id = result["cluster_id"]
    print(f"\nSubmitted job — cluster ID: {cluster_id}")


    # -----------------------------------------------------------------------
    # 5. Hold the job we just submitted
    # -----------------------------------------------------------------------

    result = json.loads(act_on_jobs.forward(
        action="Hold",
        constraint=f"ClusterId == {cluster_id}",
    ))
    print(f"Hold result: {result.get('result', result.get('error'))}")


    # -----------------------------------------------------------------------
    # 6. Release it again
    # -----------------------------------------------------------------------

    result = json.loads(act_on_jobs.forward(
        action="Release",
        constraint=f"ClusterId == {cluster_id}",
    ))
    print(f"Release result: {result.get('result', result.get('error'))}")


    # -----------------------------------------------------------------------
    # 7. Remove it
    # -----------------------------------------------------------------------

    result = json.loads(act_on_jobs.forward(
        action="Remove",
        constraint=f"ClusterId == {cluster_id}",
    ))
    print(f"Remove result: {result.get('result', result.get('error'))}")


    # -----------------------------------------------------------------------
    # 8. Read the job event log written above
    # -----------------------------------------------------------------------

    result = json.loads(read_job_events.forward(
        log_file="/tmp/mcp_htcondor_test.log",
        stop_after=0,   # non-blocking — return whatever is already in the file
    ))
    print(f"\nEvents in log ({result['count']}):")
    for ev in result["events"]:
        print(f"  {ev['type']:30s}  {ev['cluster']}.{ev['proc']}  {ev.get('timestamp', '')}")


# ---------------------------------------------------------------------------
# 9. Submit a DAG  (only runs if the .dag file actually exists)
# ---------------------------------------------------------------------------

dag_file = "/tmp/example.dag"
import os
if os.path.exists(dag_file):
    result = json.loads(submit_dag.forward(dag_file=dag_file))
    if "error" in result:
        print(f"\nDAG submit failed: {result['error']}")
    else:
        print(f"\nDAG submitted — cluster ID: {result['cluster_id']}")
else:
    print(f"\nSkipping DAG submit ({dag_file} not found)")


# ---------------------------------------------------------------------------
# 10. Read specific HTCondor config parameters
# ---------------------------------------------------------------------------

for param in ("COLLECTOR_HOST", "SCHEDD_HOST", "MAX_JOBS_RUNNING"):
    result = json.loads(get_config.forward(param_name=param))
    value = result.get(param, result.get("error", "not set"))
    print(f"  {param} = {value}")
