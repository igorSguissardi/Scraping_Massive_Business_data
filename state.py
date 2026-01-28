# Responsible for defining the shared data structure (State) across all agents in the graph.
from typing import Annotated, TypedDict, List

class GraphState(TypedDict):
    """
    Represents the state of our corporate intelligence discovery process.
    """
    initial_url: str
    # List of companies found in the ranking
    companies: List[dict] 
    # Log of agent decisions and execution steps for traceability [cite: 104, 105]
    execution_logs: List[str]