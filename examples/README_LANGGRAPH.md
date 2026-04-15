# LangGraph vs SmoLAgents Implementation Comparison

This document explains the differences between the original `htcondor_job_status.py` (using smolagents) and the new `htcondor_job_status_langgraph.py` (using LangGraph).

## Overview

Both implementations create an agent that can analyze HTCondor job scheduler status using the same underlying HTCondor tools. The main difference is in the agent framework used.

## Key Differences

### 1. **Agent Framework**

**SmoLAgents (Original):**
```python
from smolagents import CodeAgent

job_agent = CodeAgent(
    model=model,
    tools=tools,
    additional_authorized_imports=["os", "glob", "pandas", "json"],
    name="job_inspector",
    description="Summarizes the status of HTCondor jobs",
    instructions="...",
    verbosity_level=1,
)
```

**LangGraph (New):**
```python
from langgraph.prebuilt import create_react_agent

agent_executor = create_react_agent(
    model,
    langchain_tools,
    state_modifier=system_message,
)
```

### 2. **Tool Format**

**SmoLAgents:**
- Tools are already in smolagents `Tool` format
- Used directly without conversion

**LangGraph:**
- Requires LangChain-compatible tools
- Includes a conversion function `smolagents_tool_to_langchain()` that:
  - Extracts tool metadata (name, description, inputs)
  - Creates a LangChain tool wrapper
  - Maps parameter types and annotations

### 3. **Model Integration**

**SmoLAgents:**
```python
from models import get_model
model = get_model(model_id, test_connection=True)
```

**LangGraph:**
```python
from langchain_anthropic import ChatAnthropic
model = ChatAnthropic(
    model="claude-4-5-sonnet",
    temperature=0,
)
```

### 4. **Execution Pattern**

**SmoLAgents:**
```python
job_agent.run("Find all jobs from lsstsvc1...")
```

**LangGraph:**
```python
result = agent_executor.invoke(
    {"messages": [("user", query)]},
)
```

### 5. **Agent Architecture**

**SmoLAgents CodeAgent:**
- Purpose-built for code generation and execution
- Can execute Python code snippets
- Includes `additional_authorized_imports` for safety

**LangGraph ReAct Agent:**
- Graph-based agent architecture
- Uses ReAct (Reasoning + Acting) pattern
- More flexible for complex workflows
- Better observability and debugging with graph visualization
- Can be extended with custom nodes and edges

## Advantages of Each Approach

### SmoLAgents CodeAgent
- ✅ Simpler setup for code-centric tasks
- ✅ Built-in code execution capabilities
- ✅ Good for data analysis and scripting
- ✅ Lightweight dependency

### LangGraph ToolCallingAgent
- ✅ More flexible architecture
- ✅ Better for complex, multi-step workflows
- ✅ Graph-based design allows custom routing logic
- ✅ Strong ecosystem integration (LangChain, LangSmith)
- ✅ Better observability and debugging
- ✅ Can combine multiple agent patterns
- ✅ Production-ready with streaming, checkpointing, and human-in-the-loop

## Dependencies

### SmoLAgents Version
```toml
dependencies = [
    "htcondor>=24.0",
    "mcp[cli]>=1.0",
    "smolagents>=1.0",
]
```

### LangGraph Version (Additional)
```toml
dependencies = [
    "htcondor>=24.0",
    "mcp[cli]>=1.0",
    "smolagents>=1.0",  # Still needed for the tool definitions
    "langgraph>=0.2.0",
    "langchain-anthropic>=0.1.0",
    "langchain-core>=0.3.0",
]
```

## Running the Examples

### Install dependencies:
```bash
pip install -e .
```

### Run the smolagents version:
```bash
python examples/htcondor_job_status.py
```

### Run the LangGraph version:
```bash
python examples/htcondor_job_status_langgraph.py
```

## When to Use Which?

**Use SmoLAgents** if:
- You need code generation and execution
- You want a simpler, more lightweight solution
- Your use case is primarily data analysis or scripting

**Use LangGraph** if:
- You need complex multi-step workflows
- You want better observability and debugging
- You plan to add custom routing logic
- You need production features (streaming, checkpoints, human-in-the-loop)
- You want to integrate with the broader LangChain ecosystem

## Future Enhancements

The LangGraph implementation can be extended with:
- **Custom nodes** for specialized processing
- **Conditional routing** based on job status
- **Memory/persistence** for long-running workflows
- **Streaming** for real-time updates
- **Human-in-the-loop** for approval workflows
- **Graph visualization** for debugging
