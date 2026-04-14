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
    GetLogPathTool,
    ListAvailableLogsTool,
    LocateScheddsTool,
    QueryJobHistoryTool,
    QueryJobsTool,
    ReadDaemonLogTool,
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
list_logs        = ListAvailableLogsTool()
get_log_path     = GetLogPathTool()
read_daemon_log  = ReadDaemonLogTool()


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


# ---------------------------------------------------------------------------
# 11. List all available daemon logs
# ---------------------------------------------------------------------------

print("\n" + "="*60)
print("DAEMON LOG INSPECTION EXAMPLES")
print("="*60)

result = json.loads(list_logs.forward())
print(f"\nFound {result['count']} configured daemon log(s):")
for log_entry in result["logs"]:
    exists_marker = "✓" if log_entry.get("exists") else "✗"
    print(f"  {exists_marker} {log_entry['type']:20s}  {log_entry.get('path', 'not configured')}")


# ---------------------------------------------------------------------------
# 12. Get the path for a specific daemon log
# ---------------------------------------------------------------------------

result = json.loads(get_log_path.forward(log_type="SCHEDD_LOG"))
if "error" in result:
    print(f"\nGet SCHEDD_LOG path failed: {result['error']}")
else:
    print(f"\nSCHEDD_LOG configuration:")
    print(f"  Path: {result.get('path')}")
    print(f"  Exists: {result.get('exists')}")


# ---------------------------------------------------------------------------
# 13. Read last 20 lines from the Schedd log
# ---------------------------------------------------------------------------

# Only try to read if the log exists
result = json.loads(get_log_path.forward(log_type="SCHEDD_LOG"))
if not result.get("error") and result.get("exists"):
    schedd_log_path = result["path"]

    result = json.loads(read_daemon_log.forward(
        log_path=schedd_log_path,
        lines=20,
    ))

    if "error" in result:
        print(f"\nRead SCHEDD_LOG failed: {result['error']}")
    else:
        print(f"\nLast {result['count']} lines from SCHEDD_LOG:")
        print(f"  (Truncated: {result['truncated']})")
        for i, line in enumerate(result["lines"], 1):
            # Show just first 100 chars to keep output readable
            display_line = line[:100] + "..." if len(line) > 100 else line
            print(f"  {i:2d}. {display_line}")
else:
    print("\nSkipping SCHEDD_LOG read (log not configured or doesn't exist)")


# ---------------------------------------------------------------------------
# 14. Search for ERROR or WARNING messages in Schedd log
# ---------------------------------------------------------------------------

result = json.loads(get_log_path.forward(log_type="SCHEDD_LOG"))
if not result.get("error") and result.get("exists"):
    schedd_log_path = result["path"]

    result = json.loads(read_daemon_log.forward(
        log_path=schedd_log_path,
        lines=100,
        filter_pattern=r"ERROR|WARNING|WARN",
    ))

    if "error" in result:
        print(f"\nSearch SCHEDD_LOG for errors failed: {result['error']}")
    else:
        print(f"\nFound {result['count']} ERROR/WARNING message(s) in last 100 lines of SCHEDD_LOG:")
        for i, line in enumerate(result["lines"][:10], 1):  # Show first 10
            display_line = line[:120] + "..." if len(line) > 120 else line
            print(f"  {i:2d}. {display_line}")
        if result['count'] > 10:
            print(f"  ... and {result['count'] - 10} more")
else:
    print("\nSkipping SCHEDD_LOG error search (log not configured or doesn't exist)")


# ---------------------------------------------------------------------------
# 15. Read Collector log if available
# ---------------------------------------------------------------------------

result = json.loads(get_log_path.forward(log_type="COLLECTOR_LOG"))
if not result.get("error") and result.get("exists"):
    collector_log_path = result["path"]

    result = json.loads(read_daemon_log.forward(
        log_path=collector_log_path,
        lines=10,
    ))

    if "error" in result:
        print(f"\nRead COLLECTOR_LOG failed: {result['error']}")
    else:
        print(f"\nLast {result['count']} lines from COLLECTOR_LOG:")
        for i, line in enumerate(result["lines"], 1):
            display_line = line[:100] + "..." if len(line) > 100 else line
            print(f"  {i:2d}. {display_line}")
else:
    print("\nSkipping COLLECTOR_LOG read (log not configured or doesn't exist)")
