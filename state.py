# Responsible for defining the shared data structure (State) across all agents in the graph.
import operator
from typing import Annotated, List, Optional, TypedDict


class CompanyRecord(TypedDict, total=False):
    """
    Define enrichment payload for company.
    """
    classificacao_2024: str
    classificacao_2023: str
    nome_empresa: str
    sede: str
    setor: str
    receita_liquida_milhoes: str
    lucro_liquido_milhoes: str
    razao_social: Optional[str]
    official_website: Optional[str]
    linkedin_url: Optional[str]
    about_page_url: Optional[str]
    primary_cnpj: Optional[str]
    found_brands: List[str]
    corporate_group_notes: Optional[str]


class GraphState(TypedDict):
    """
    Define shared state for corporate intelligence pipeline.
    """
    # Preserve origin endpoint so replay stay deterministic
    initial_url: str
    # Use annotated aggregator so enrichment stage preserves prior insight
    companies: Annotated[List[CompanyRecord], operator.add]
    # Staged list for per-company fan-out processing
    company_queue: List[CompanyRecord]
    # Apply annotated aggregator so audit trail remains cumulative
    execution_logs: Annotated[List[str], operator.add]
    # Track LLM API calls for cost auditing and rate limit management
    llm_request_count: int
    # Store CSV sniper results for corporate structure enrichment
    corporate_csv_evidence: Annotated[List[Optional[str]], operator.add]
    # Store institutional content distilled into markdown
    institutional_markdown: Annotated[List[Optional[str]], operator.add]
    # Store institutional page summaries per company
    institutional_summary: Annotated[List[Optional[str]], operator.add]


class CompanyState(TypedDict):
    """
    Define per-company state for fan-out processing.
    """
    company: CompanyRecord
    execution_logs: Annotated[List[str], operator.add]
    institutional_markdown: Annotated[List[Optional[str]], operator.add]
    institutional_summary: Annotated[List[Optional[str]], operator.add]
    corporate_csv_evidence: Annotated[List[Optional[str]], operator.add]
