# Responsible for defining the shared data structure (State) across all agents in the graph.
import operator
from typing import Annotated, List, Optional, TypedDict


def _coalesce_str(left: str, right: str) -> str:
    if left:
        return left
    return right


def _max_int(left: int, right: int) -> int:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


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
    physical_address: Optional[str]
    institutional_description: Optional[str]
    institutional_summary: Optional[str]
    primary_cnpj: Optional[str]
    found_brands: List[str]
    corporate_group_notes: Optional[str]
    relationships: List[dict]
    origin_company: Optional[bool]
    run_id: Optional[str]
    log_file: Optional[str]


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
    # Track LLM API call count across fan-out branches
    llm_request_count: Annotated[int, operator.add]
    # Track LLM token usage across fan-out branches
    llm_input_tokens: Annotated[int, operator.add]
    llm_output_tokens: Annotated[int, operator.add]
    llm_total_tokens: Annotated[int, operator.add]
    # Track LLM cost (USD) across fan-out branches
    llm_cost_usd: Annotated[float, operator.add]
    # Store CSV sniper results for corporate structure enrichment
    corporate_csv_evidence: Annotated[List[Optional[str]], operator.add]
    # Store institutional content distilled into markdown
    institutional_markdown: Annotated[List[Optional[str]], operator.add]
    # Store institutional page summaries per company
    institutional_summary: Annotated[List[Optional[str]], operator.add]
    # Track which companies have been ingested into Neo4j
    ingested_company_ids: Annotated[List[str], operator.add]
    # Neo4j batch ingest coordination
    neo4j_expected_total: Annotated[int, _max_int]
    neo4j_batch_token: Annotated[str, _coalesce_str]


class CompanyState(TypedDict):
    """
    Define per-company state for fan-out processing.
    """
    companies: Annotated[List[CompanyRecord], operator.add]
    company: CompanyRecord
    execution_logs: Annotated[List[str], operator.add]
    institutional_markdown: Annotated[List[Optional[str]], operator.add]
    institutional_summary: Annotated[List[Optional[str]], operator.add]
    corporate_csv_evidence: Annotated[List[Optional[str]], operator.add]
    llm_request_count: Annotated[int, operator.add]
    llm_input_tokens: Annotated[int, operator.add]
    llm_output_tokens: Annotated[int, operator.add]
    llm_total_tokens: Annotated[int, operator.add]
    llm_cost_usd: Annotated[float, operator.add]
    # Neo4j batch ingest coordination (propagated per-company)
    neo4j_expected_total: int
    neo4j_batch_token: str
    ingested_company_ids: Annotated[List[str], operator.add]
