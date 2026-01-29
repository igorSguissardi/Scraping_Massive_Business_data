# Responsible for defining the shared data structure (State) across all agents in the graph.
import operator
from typing import Annotated, TypedDict, List

class GraphState(TypedDict):
    """
    Represents the state of our corporate intelligence discovery process.
    """
    initial_url: str
    # Company list found in the ranking
    companies: Annotated[List[dict], operator.add]
    # Log entry archive for agent decision traceability [cite: 104, 105]
    execution_logs: Annotated[List[str], operator.add]