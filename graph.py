# Responsible for orchestrating the flow between agents, defining edges, and compiling the state machine.
from langgraph.graph import StateGraph, START, END
from state import GraphState
from nodes import ranking_scraper_node

# Initialize the state machine with our shared state structure
workflow = StateGraph(GraphState)

# Add the nodes defined in nodes.py
workflow.add_node("ranking_scraper", ranking_scraper_node)

# Define the flow logic
# We start at the scraper and, for now, we finish the execution after it
workflow.add_edge(START, "ranking_scraper")
workflow.add_edge("ranking_scraper", END)

# Compile the graph into an executable application
app = workflow.compile()