# Responsible for orchestrating the flow between agents, defining edges, and compiling the state machine.
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send
from state import GraphState, CompanyState
from nodes import (
    ranking_scraper_node,
    limit_companies_node,
    prepare_company_fanout,
    enrichment_company_node,
    institutional_company_node,
    neo4j_ingest_node,
)

# Initialize the state machine with our shared state structure
workflow = StateGraph(GraphState)

# Add the nodes defined in nodes.py
workflow.add_node("ranking_scraper", ranking_scraper_node)
workflow.add_node("limit_companies", limit_companies_node)
workflow.add_node("prepare_fanout", prepare_company_fanout)
workflow.add_node("neo4j_ingest", neo4j_ingest_node)

# Per-company subgraph
company_workflow = StateGraph(CompanyState)
company_workflow.add_node("enrichment_company", enrichment_company_node)
company_workflow.add_node("institutional_company", institutional_company_node)
company_workflow.add_edge(START, "enrichment_company")
company_workflow.add_edge("enrichment_company", "institutional_company")
company_workflow.add_edge("institutional_company", END)
company_pipeline = company_workflow.compile()
workflow.add_node("company_pipeline", company_pipeline)

# Define the flow logic
workflow.add_edge(START, "ranking_scraper")
workflow.add_edge("ranking_scraper", "limit_companies")
workflow.add_edge("limit_companies", "prepare_fanout")


def dispatch_companies(state: GraphState):
    companies = state.get("company_queue", [])
    return [
        Send(
            "company_pipeline",
            {
                "company": company,
                "execution_logs": [],
                "institutional_markdown": [],
            },
        )
        for company in companies
    ]


workflow.add_conditional_edges("prepare_fanout", dispatch_companies)
workflow.add_edge("prepare_fanout", END)
workflow.add_edge("company_pipeline", "neo4j_ingest")
workflow.add_edge("company_pipeline", END)
workflow.add_edge("neo4j_ingest", END)

# Compile the graph into an executable application
app = workflow.compile()
