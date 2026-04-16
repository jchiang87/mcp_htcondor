import logging
from models import get_model
from smolagents import CodeAgent
from mcp_htcondor import (
    LocateScheddsTool,
    QueryJobsTool,
    QueryJobHistoryTool,
    ListAvailableLogsTool,
    ReadJobEventsTool,
    GetHtcondorConfigTool,
    GetLogPathTool,
    ReadDaemonLogTool,
)

logging.getLogger("smolagents").setLevel(logging.FATAL)

tools = [
    query_jobs := QueryJobsTool(),
    query_history := QueryJobHistoryTool(),
#    submit_job := SubmitJobTool(),
#    submit_dag := SubmitDagTool(),
#    act_on_jobs := ActOnJobsTool(),
    locate_schedds := LocateScheddsTool(),
    read_job_events := ReadJobEventsTool(),
    get_config := GetHtcondorConfigTool(),
    list_logs := ListAvailableLogsTool(),
    get_log_path := GetLogPathTool(),
    read_daemon_log := ReadDaemonLogTool(),
]

model_id = "claude-4-5-sonnet"
model = get_model(model_id, test_connection=True)

job_agent = CodeAgent(
    model=model,
    tools=tools,
    additional_authorized_imports=["os", "glob", "pandas", "json"],
    name="job_inspector",
    description="Summarizes the status of HTCondor jobs",
    instructions=("Aggregate information on HTCondor jobs running on "
                  "the various schedds."),
    verbosity_level=1,
)

job_agent.run("""
Find all of the jobs from lsstsvc1, disaggregated by schedd and bps_job_label.
""")
