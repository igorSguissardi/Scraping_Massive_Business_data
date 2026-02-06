# Responsible for orchestrating the flow between agents, defining edges, and compiling the state machine.
from langgraph.graph import StateGraph, START, END
from state import GraphState
from nodes import ranking_scraper_node, limit_companies_node, enrichment_node, institutional_scraping_node # Add enrichment_node here

# Initialize the state machine with our shared state structure
workflow = StateGraph(GraphState)

# Add the nodes defined in nodes.py
workflow.add_node("ranking_scraper", ranking_scraper_node)
workflow.add_node("limit_companies", limit_companies_node)
workflow.add_node("enrichment", enrichment_node) # Register the new node
workflow.add_node("institutional_scraping", institutional_scraping_node)

# Define the flow logic
workflow.add_edge(START, "ranking_scraper")
workflow.add_edge("ranking_scraper", "limit_companies")
workflow.add_edge("limit_companies", "enrichment") # 2nd: Pass list to Enricher
workflow.add_edge("enrichment", "institutional_scraping")
workflow.add_edge("institutional_scraping", END)

# Compile the graph into an executable application
app = workflow.compile()
