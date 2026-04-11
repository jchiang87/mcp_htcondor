"""HTCondor tools using the smolagents Tool interface.

Each class wraps a coherent set of HTCondor Python binding calls in a
smolagents-compatible Tool that can be used directly with any smolagents
agent or exposed via an MCP server.
"""

from __future__ import annotations

import json
from typing import Optional

import classad
import htcondor
from smolagents import Tool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _locate_schedd(schedd_name: Optional[str] = None) -> htcondor.Schedd:
    """Return a Schedd handle, looking it up via the local Collector."""
    coll = htcondor.Collector()
    if schedd_name:
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
    else:
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
    return htcondor.Schedd(schedd_ad)


def _ad_to_dict(ad) -> dict:
    """Convert a ClassAd to a JSON-serialisable dict.

    classad.ExprTree and classad.Value instances are stringified; all other
    values (str, int, float, bool, None) are passed through unchanged.
    """
    result = {}
    for key, value in dict(ad).items():
        if isinstance(value, (str, int, float, bool, type(None))):
            result[key] = value
        else:
            result[key] = str(value)
    return result


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


class QueryJobsTool(Tool):
    """Query active jobs from an HTCondor schedd."""

    name = "query_jobs"
    description = (
        "Query the HTCondor scheduler for active (queued or running) jobs. "
        "Returns a JSON object with 'jobs' (list of ClassAds) and 'count'. "
        "Use a constraint expression to filter jobs and a projection list to "
        "limit which attributes are returned."
    )
    inputs = {
        "constraint": {
            "type": "string",
            "description": (
                "ClassAd expression to filter jobs.  Examples: "
                "'Owner == \"alice\"', 'JobStatus == 2' (running), "
                "'ClusterId == 12345'.  Defaults to 'True' (all jobs)."
            ),
            "nullable": True,
        },
        "projection": {
            "type": "array",
            "description": (
                "List of job attribute names to return, e.g. "
                '["ClusterId", "ProcId", "JobStatus", "Owner", "Cmd"]. '
                "If None all attributes are returned."
            ),
            "nullable": True,
        },
        "schedd_name": {
            "type": "string",
            "description": (
                "Name of the schedd daemon to query.  Uses the local schedd "
                "when not specified."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        constraint: Optional[str] = None,
        projection: Optional[list] = None,
        schedd_name: Optional[str] = None,
    ) -> str:
        try:
            schedd = _locate_schedd(schedd_name)
            kwargs: dict = {"constraint": constraint or "True"}
            if projection:
                kwargs["projection"] = list(projection)
            jobs = [_ad_to_dict(j) for j in schedd.query(**kwargs)]
            return json.dumps({"jobs": jobs, "count": len(jobs)})
        except Exception as exc:
            return json.dumps({"error": str(exc), "jobs": [], "count": 0})


class QueryJobHistoryTool(Tool):
    """Query completed / historical jobs from an HTCondor schedd."""

    name = "query_job_history"
    description = (
        "Query the HTCondor scheduler for historical jobs (completed, removed, "
        "etc.).  Returns a JSON object with 'jobs' (list of ClassAds) and "
        "'count'.  History jobs are those no longer in the active queue."
    )
    inputs = {
        "constraint": {
            "type": "string",
            "description": (
                "ClassAd expression to filter history jobs.  Examples: "
                "'Owner == \"alice\"', 'JobStatus == 4' (completed), "
                "'ClusterId == 12345'."
            ),
            "nullable": True,
        },
        "projection": {
            "type": "array",
            "description": (
                "List of job attribute names to return.  If None all "
                "attributes are returned."
            ),
            "nullable": True,
        },
        "match": {
            "type": "integer",
            "description": (
                "Maximum number of history records to return.  Defaults to "
                "-1 (unlimited)."
            ),
            "nullable": True,
        },
        "schedd_name": {
            "type": "string",
            "description": (
                "Name of the schedd daemon to query.  Uses the local schedd "
                "when not specified."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        constraint: Optional[str] = None,
        projection: Optional[list] = None,
        match: Optional[int] = None,
        schedd_name: Optional[str] = None,
    ) -> str:
        try:
            schedd = _locate_schedd(schedd_name)
            kwargs: dict = {"constraint": constraint or "True"}
            if projection:
                kwargs["projection"] = list(projection)
            if match is not None:
                kwargs["match"] = int(match)
            jobs = [_ad_to_dict(j) for j in schedd.history(**kwargs)]
            return json.dumps({"jobs": jobs, "count": len(jobs)})
        except Exception as exc:
            return json.dumps({"error": str(exc), "jobs": [], "count": 0})


class SubmitJobTool(Tool):
    """Submit one or more jobs to an HTCondor schedd."""

    name = "submit_job"
    description = (
        "Submit one or more jobs to the HTCondor scheduler.  Accepts a submit "
        "description as a JSON object of key=value pairs (same format as an "
        "HTCondor submit file).  Returns the cluster ID of the submitted "
        "job(s)."
    )
    inputs = {
        "submit_description": {
            "type": "object",
            "description": (
                "HTCondor submit description as a JSON object.  Example: "
                '{"executable": "/bin/echo", "arguments": "hello", '
                '"output": "job.out", "error": "job.err", '
                '"log": "job.log", "queue": "1"}.'
            ),
        },
        "schedd_name": {
            "type": "string",
            "description": (
                "Name of the schedd daemon.  Uses the local schedd when not "
                "specified."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        submit_description: dict,
        schedd_name: Optional[str] = None,
    ) -> str:
        try:
            # htcondor.Submit expects string values
            str_desc = {str(k): str(v) for k, v in submit_description.items()}
            schedd = _locate_schedd(schedd_name)
            sub = htcondor.Submit(str_desc)
            result = schedd.submit(sub)
            return json.dumps({"cluster_id": result.cluster()})
        except Exception as exc:
            return json.dumps({"error": str(exc)})


class SubmitDagTool(Tool):
    """Submit a DAGMan workflow to an HTCondor schedd."""

    name = "submit_dag"
    description = (
        "Submit a DAGMan workflow to the HTCondor scheduler.  Reads a .dag "
        "file and submits it as a DAGMan job.  Returns the cluster ID of the "
        "DAGMan job."
    )
    inputs = {
        "dag_file": {
            "type": "string",
            "description": "Path to the DAG description file (.dag).",
        },
        "submit_options": {
            "type": "object",
            "description": (
                "Optional DAGMan submit options as a JSON object.  Example: "
                '{"MaxIdle": "10", "MaxJobs": "100"}.'
            ),
            "nullable": True,
        },
        "schedd_name": {
            "type": "string",
            "description": (
                "Name of the schedd daemon.  Uses the local schedd when not "
                "specified."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        dag_file: str,
        submit_options: Optional[dict] = None,
        schedd_name: Optional[str] = None,
    ) -> str:
        try:
            schedd = _locate_schedd(schedd_name)
            sub = htcondor.Submit.from_dag(dag_file, submit_options or {})
            result = schedd.submit(sub)
            return json.dumps({"cluster_id": result.cluster(), "dag_file": dag_file})
        except Exception as exc:
            return json.dumps({"error": str(exc)})


class ActOnJobsTool(Tool):
    """Perform an action (remove, hold, release, …) on HTCondor jobs."""

    name = "act_on_jobs"
    description = (
        "Perform an action on HTCondor jobs matching a ClassAd constraint.  "
        "Supported actions: Remove (graceful), RemoveX (forceful), Hold, "
        "Release, Suspend, Continue, Vacate, VacateFast.  Returns a JSON "
        "summary of the action result."
    )
    inputs = {
        "action": {
            "type": "string",
            "description": (
                "Action to perform.  One of: 'Remove', 'RemoveX', 'Hold', "
                "'Release', 'Suspend', 'Continue', 'Vacate', 'VacateFast'."
            ),
        },
        "constraint": {
            "type": "string",
            "description": (
                "ClassAd constraint identifying the target jobs.  Examples: "
                "'ClusterId == 12345', 'Owner == \"alice\"', "
                "'JobStatus == 5' (held jobs)."
            ),
        },
        "schedd_name": {
            "type": "string",
            "description": (
                "Name of the schedd daemon.  Uses the local schedd when not "
                "specified."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    _ACTION_MAP: dict[str, htcondor.JobAction] = {
        "Remove": htcondor.JobAction.Remove,
        "RemoveX": htcondor.JobAction.RemoveX,
        "Hold": htcondor.JobAction.Hold,
        "Release": htcondor.JobAction.Release,
        "Suspend": htcondor.JobAction.Suspend,
        "Continue": htcondor.JobAction.Continue,
        "Vacate": htcondor.JobAction.Vacate,
        "VacateFast": htcondor.JobAction.VacateFast,
    }

    def forward(
        self,
        action: str,
        constraint: str,
        schedd_name: Optional[str] = None,
    ) -> str:
        job_action = self._ACTION_MAP.get(action)
        if job_action is None:
            return json.dumps({
                "error": (
                    f"Unknown action '{action}'.  "
                    f"Valid actions: {list(self._ACTION_MAP)}"
                )
            })
        try:
            schedd = _locate_schedd(schedd_name)
            result = schedd.act(job_action, constraint)
            return json.dumps({
                "action": action,
                "constraint": constraint,
                "result": _ad_to_dict(result) if result else {},
            })
        except Exception as exc:
            return json.dumps({"error": str(exc)})


class LocateScheddsTool(Tool):
    """Locate HTCondor schedd daemons via the Collector."""

    name = "locate_schedds"
    description = (
        "Locate available HTCondor schedulers (schedds) by querying the "
        "Collector.  Returns a JSON object with a 'schedds' list of ClassAds "
        "including Name, Machine, TotalRunningJobs, TotalIdleJobs, and other "
        "daemon attributes."
    )
    inputs = {
        "schedd_name": {
            "type": "string",
            "description": (
                "Locate a specific schedd by name.  If not specified, all "
                "available schedds are returned."
            ),
            "nullable": True,
        },
        "constraint": {
            "type": "string",
            "description": "ClassAd constraint to filter Collector results.",
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        schedd_name: Optional[str] = None,
        constraint: Optional[str] = None,
    ) -> str:
        try:
            coll = htcondor.Collector()
            if schedd_name:
                schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
                schedds = [_ad_to_dict(schedd_ad)]
            elif constraint:
                ads = coll.query(constraint=constraint)
                schedds = [_ad_to_dict(ad) for ad in ads]
            else:
                ads = coll.locateAll(htcondor.DaemonTypes.Schedd)
                schedds = [_ad_to_dict(ad) for ad in ads]
            return json.dumps({"schedds": schedds, "count": len(schedds)})
        except Exception as exc:
            return json.dumps({"error": str(exc), "schedds": [], "count": 0})


class ReadJobEventsTool(Tool):
    """Read events from an HTCondor job event log file."""

    name = "read_job_events"
    description = (
        "Read events from an HTCondor job event log file.  Returns a JSON "
        "object with an 'events' list.  Each event includes the type name, "
        "cluster/proc IDs, timestamp, and all other event attributes."
    )
    inputs = {
        "log_file": {
            "type": "string",
            "description": "Path to the HTCondor job event log file.",
        },
        "stop_after": {
            "type": "number",
            "description": (
                "Seconds to wait for new events.  0 = non-blocking (default), "
                "-1 = block until a new event arrives."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(
        self,
        log_file: str,
        stop_after: Optional[float] = None,
    ) -> str:
        try:
            jel = htcondor.JobEventLog(log_file)
            wait = 0.0 if stop_after is None else float(stop_after)
            events = []
            for event in jel.events(stop_after=wait):
                entry: dict = {
                    "type": event.type.name,
                    "type_number": int(event.type),
                    "cluster": event.get("Cluster"),
                    "proc": event.get("Proc"),
                    "timestamp": event.get("EventTime", ""),
                }
                try:
                    for key in event.keys():
                        if key not in ("Cluster", "Proc", "EventTime"):
                            try:
                                entry[key] = event[key]
                            except Exception:
                                pass
                except Exception:
                    pass
                events.append(entry)
            return json.dumps({"events": events, "count": len(events), "log_file": log_file})
        except Exception as exc:
            return json.dumps({"error": str(exc), "events": [], "count": 0})


class GetHtcondorConfigTool(Tool):
    """Read HTCondor configuration parameters."""

    name = "get_htcondor_config"
    description = (
        "Read HTCondor configuration parameters.  If a parameter name is "
        "provided, returns its value.  Otherwise returns all configuration "
        "parameters as a JSON object."
    )
    inputs = {
        "param_name": {
            "type": "string",
            "description": (
                "Name of a specific parameter to read, e.g. 'SCHEDD_HOST', "
                "'COLLECTOR_HOST', 'MAX_JOBS_RUNNING'.  If not specified, all "
                "parameters are returned."
            ),
            "nullable": True,
        },
    }
    output_type = "string"

    def forward(self, param_name: Optional[str] = None) -> str:
        try:
            if param_name:
                value = htcondor.param.get(param_name)
                return json.dumps({param_name: value})
            return json.dumps(dict(htcondor.param))
        except Exception as exc:
            return json.dumps({"error": str(exc)})
