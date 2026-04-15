"""HTCondor job status agent using LangGraph ToolCallingAgent.

This re-implements the smolagents CodeAgent example using LangGraph's
create_react_agent, which provides a ReAct-style agent with tool calling.
"""

import logging
from typing import Annotated

from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def smolagents_tool_to_langchain(smolagent_tool):
    """Convert a smolagents Tool to a LangChain tool.

    Creates a LangChain tool that wraps the smolagents tool's forward method
    and uses its name, description, and inputs for the schema.
    """
    from typing import Optional, Union, List, Dict, Any

    # Get the tool's metadata
    tool_name = smolagent_tool.name
    tool_description = smolagent_tool.description

    # Build the inputs signature for the LangChain tool
    # smolagents tools have an `inputs` dict that defines the parameters
    inputs_dict = smolagent_tool.inputs

    # Create a wrapper function that will become the LangChain tool
    def tool_func(**kwargs):
        """Wrapper that calls the smolagents tool's forward method."""
        try:
            result = smolagent_tool.forward(**kwargs)
            return result
        except Exception as e:
            logger.error(f"Error calling {tool_name}: {e}")
            return f"Error: {str(e)}"

    # Set the function name and docstring
    tool_func.__name__ = tool_name
    tool_func.__doc__ = tool_description

    # Build annotations for the function parameters from smolagents inputs
    annotations = {}
    for param_name, param_info in inputs_dict.items():
        # smolagents defines param_info as dicts with 'type', 'description', 'nullable'
        param_type = param_info.get('type', 'text')
        param_desc = param_info.get('description', '')
        nullable = param_info.get('nullable', False)

        # Map smolagents types to Python types
        type_mapping = {
            'text': str,
            'string': str,
            'integer': int,
            'number': float,
            'boolean': bool,
            'array': List[Any],
            'object': Dict[str, Any],
        }
        python_type = type_mapping.get(param_type, str)

        # Handle nullable parameters with Optional
        if nullable:
            python_type = Optional[python_type]

        # Use Annotated to add description metadata
        annotations[param_name] = Annotated[python_type, param_desc]

    # Update function annotations
    tool_func.__annotations__ = annotations

    # Create the LangChain tool using the @tool decorator pattern
    return tool(tool_func)


def main():
    """Run the HTCondor job status agent."""

    # Initialize the smolagents HTCondor tools
    smolagent_tools = [
        QueryJobsTool(),
        QueryJobHistoryTool(),
        LocateScheddsTool(),
        ReadJobEventsTool(),
        GetHtcondorConfigTool(),
        ListAvailableLogsTool(),
        GetLogPathTool(),
        ReadDaemonLogTool(),
    ]

    # Convert smolagents tools to LangChain tools
    logger.info("Converting smolagents tools to LangChain format...")
    langchain_tools = [smolagents_tool_to_langchain(t) for t in smolagent_tools]

    # Initialize the Claude model via LangChain
    model = ChatAnthropic(
        model="claude-4-5-sonnet",
        temperature=0,
    )

    # Create the system prompt
    system_message = """You are a helpful assistant for analyzing HTCondor job scheduler status.

Your role is to aggregate information on HTCondor jobs running on various schedds (schedulers).
You have access to tools that can query job queues, job history, read log files, and inspect
HTCondor configuration.

When analyzing jobs:
1. Start by locating available schedds if needed
2. Query jobs from the relevant schedds
3. Aggregate and summarize the results clearly
4. If you need more details, use the job history or log reading tools

Be thorough but concise in your analysis."""

    # Create the ReAct agent using LangGraph's create_react_agent
    logger.info("Creating LangGraph agent...")
    agent_executor = create_react_agent(
        model,
        langchain_tools,
        state_modifier=system_message,
    )

    # Run the agent with the user query
    query = """
Find all of the jobs from lsstsvc1, disaggregated by schedd and bps_job_label.
"""

    logger.info(f"Running agent with query: {query.strip()}")
    logger.info("=" * 80)

    # Execute the agent
    result = agent_executor.invoke(
        {"messages": [("user", query)]},
    )

    # Print the final response
    logger.info("=" * 80)
    logger.info("Agent response:")
    logger.info("=" * 80)

    # The result contains all messages; get the last assistant message
    for message in result["messages"]:
        if hasattr(message, "type") and message.type == "ai":
            print(message.content)
        elif hasattr(message, "content"):
            # For the final output
            if "user" not in str(type(message)).lower():
                print(message.content)


if __name__ == "__main__":
    main()
