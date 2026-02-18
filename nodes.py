# Responsible for housing the individual functions (nodes) that perform specific tasks like scraping and analysis.
import asyncio
import json
import os
import re
import requests
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import html2text

from state import GraphState
from utils.parser import parse_valor_1000_json
from utils.tools import (
    get_search_query,
    search_company_web_presence,
    get_filtered_csv_data,
    get_shareholding_owns_relationships,
    build_cnpj_retry_queries,
    has_cnpj_in_results,
    merge_search_results,
    log_company_event,
)
from utils.neo4j_ingest import ingest_companies_batch

# Lazy-load LLM on first use to avoid initialization errors when API key is not set
_enrichment_llm = None
_MAX_COMPANIES = 10
NEO4J_BATCH_SIZE = int(os.getenv("NEO4J_BATCH_SIZE", "10"))


def prepare_company_fanout(state: GraphState):
    """
    Stage companies for per-company fan-out.
    """
    source_companies = state.get("company_queue", [])
    return {
        "company_queue": source_companies,
        "execution_logs": [f"Prepared {len(source_companies)} companies for per-company processing."],
    }


def enrichment_company_node(state: dict):
    """
    Run enrichment for a single company by reusing the batch enrichment node.
    """
    company = state.get("company")
    if not company:
        return {"execution_logs": ["[SKIPPED] enrichment_company_node: Missing company payload."]}

    temp_state = {
        "companies": [company],
        "execution_logs": [],
        "institutional_markdown": [],
    }
    result = enrichment_node(temp_state)
    enriched_companies = result.get("companies", [])
    enriched_company = enriched_companies[0] if enriched_companies else company
    corporate_csv_evidence = result.get("corporate_csv_evidence")
    llm_request_count = result.get("llm_request_count", 0)
    return {
        "company": enriched_company,
        "execution_logs": result.get("execution_logs", []),
        "corporate_csv_evidence": [corporate_csv_evidence],
        "llm_request_count": llm_request_count,
    }


async def institutional_company_node(state: dict):
    """
    Run institutional scraping for a single company by reusing the batch node.
    """
    company = state.get("company")
    company_name = company.get("nome_empresa", "Unknown") if company else "Unknown"
    if not company:
        return {"execution_logs": ["[SKIPPED] institutional_company_node: Missing company payload."]}

    temp_state = {
        "companies": [company],
        "execution_logs": [],
        "institutional_markdown": [],
    }
    result = await institutional_scraping_node(temp_state)
    markdown_list = result.get("institutional_markdown", [])
    markdown = markdown_list[0] if markdown_list else None

    summary = None
    summary_logs = []
    llm_request_count = 0
    node_label = "institutional_summary"
    if markdown:
        try:
            enrichment_llm = get_enrichment_llm()
            llm_request_count += 1
            prompt_template = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        """You are a Corporate Intelligence Analyst. Your goal is to generate a structured 'Institutional Description' (Descrição Institucional) for a Brazilian company based on its web content.
                        INSTRUCTIONS:
                        1. Identify the core Business Model (B2B, B2C, Marketplace, etc.).
                        2. Extract the primary products and services, normalizing different market terminologies into clear categories (e.g., convert 'gateway' or 'adquirência' into 'Payment Solutions').
                        3. Define the Ideal Customer Profile (ICP) and the target industry/sector.
                        4. If the text is insufficient, non-descriptive, or contains only legal/cookie warnings, return exactly: null.
                        OUTPUT FORMAT:
                        Provide a concise 3-sentence summary that covers: [Business Model] + [Core Products/Services] + [Target Audience/ICP]. Use professional, objective language.""",
                    ),
                    ("human", "Institutional markdown:\n{markdown}"),
                ]
            )
            chain = prompt_template | enrichment_llm
            llm_response = chain.invoke({"markdown": markdown})
            summary_text = getattr(llm_response, "content", str(llm_response)).strip()
            if summary_text and summary_text.lower() != "null":
                summary = summary_text
                log_company_event(
                    company,
                    node_label,
                    f"[SUMMARY] {company_name}: {summary}",
                    execution_logs=summary_logs,
                    also_print=True,
                )
            else:
                log_company_event(
                    company,
                    node_label,
                    f"[SUMMARY] {company_name}: Not available",
                    execution_logs=summary_logs,
                    also_print=True,
                )
        except Exception as exc:
            log_company_event(
                company,
                node_label,
                f"[SUMMARY] {company_name}: Failed ({exc})",
                execution_logs=summary_logs,
                also_print=True,
            )
    else:
        log_company_event(
            company,
            node_label,
            f"[SUMMARY] {company_name}: Not available",
            execution_logs=summary_logs,
            also_print=True,
        )

    if isinstance(company, dict):
        company["institutional_summary"] = summary
    return {
        "company": company,
        "companies": [company],
        "institutional_markdown": markdown_list,
        "institutional_summary": [summary],
        "execution_logs": result.get("execution_logs", []) + summary_logs,
        "llm_request_count": llm_request_count,
    }


async def neo4j_ingest_node(state: GraphState):
    """
    Batch-ingest enriched companies into Neo4j using UNWIND.
    """
    companies = state.get("companies", [])
    ingested_ids = set(state.get("ingested_company_ids", []))
    def _normalize_cnpj(value: str) -> str | None:
        if value is None:
            return None
        text = re.sub(r"\D", "", str(value))
        if len(text) != 14 or not text.isdigit():
            return None
        return text
    pending = []
    seen_ids = set()
    for company in companies:
        normalized = _normalize_cnpj(company.get("primary_cnpj"))
        if not normalized:
            continue
        if normalized in ingested_ids:
            continue
        if normalized in seen_ids:
            continue
        if isinstance(company, dict) and company.get("primary_cnpj") != normalized:
            company = dict(company)
            company["primary_cnpj"] = normalized
        pending.append(company)
        seen_ids.add(normalized)

    if not pending:
        if companies:
            return {
                "execution_logs": [
                    f"[NEO4J] Skipped ingestion: 0 pending with valid 14-digit CNPJ out of {len(companies)} companies."
                ]
            }
        return {}

    total_expected = len(state.get("company_queue") or [])
    total_ready = len(companies)
    if total_expected == 0:
        # Fall back when fan-out context is missing (e.g., single-company runs)
        total_expected = total_ready
    # In fan-out mode each branch only sees a subset, so flush immediately.
    fanout_partial = total_expected > 0 and total_ready < total_expected
    should_flush = fanout_partial or len(pending) >= NEO4J_BATCH_SIZE or total_ready >= total_expected
    if not should_flush:
        return {
            "execution_logs": [
                f"[NEO4J] Waiting for more companies (ready {total_ready}/{total_expected}, pending {len(pending)})."
            ]
        }

    try:
        all_ingested = []
        batch_count = 0
        offset = 0
        while offset < len(pending):
            batch = pending[offset : offset + NEO4J_BATCH_SIZE]
            ingested = await asyncio.to_thread(ingest_companies_batch, batch)
            if not ingested:
                print("[NEO4J] No valid company IDs to ingest in this batch.")
                if not all_ingested:
                    return {"execution_logs": ["[NEO4J] No valid company IDs to ingest in this batch."]}
                break
            batch_count += 1
            all_ingested.extend(ingested)
            ingested_ids.update(ingested)
            offset += len(batch)
        print(f"[NEO4J] Ingested {len(all_ingested)} companies in {batch_count} batch(es).")
        return {
            "ingested_company_ids": all_ingested,
            "execution_logs": [f"[NEO4J] Ingested {len(all_ingested)} companies in {batch_count} batch(es)."],
        }
    except Exception as exc:
        print(f"[NEO4J] Batch ingestion failed: {exc}")
        return {"execution_logs": [f"[NEO4J] Batch ingestion failed: {exc}"]}


def get_enrichment_llm():
    """
    Instantiate the LLM only when first needed, avoiding import-time API key errors.
    Reuse the same instance across invocations for efficiency.
    """
    global _enrichment_llm
    if _enrichment_llm is None:
        # Explicitly read the API key from environment
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable is not set. "
                "Please ensure the .env file exists and contains: OPENAI_API_KEY=your_key"
            )
        
        # Pass API key explicitly to ChatOpenAI
        _enrichment_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)
    return _enrichment_llm


def limit_companies_node(state: GraphState):
    """
    Limit the number of companies processed in downstream nodes.
    """

    print("--- NODE: Limit Companies ---")
    source_companies = state.get("company_queue", [])
    limited_companies = source_companies[:_MAX_COMPANIES]
    log_message = (
        f"Limited companies from {len(source_companies)} to {len(limited_companies)} for processing."
    )

    return {
        "company_queue": limited_companies,
        "execution_logs": [log_message],
    }



def ranking_scraper_node(state: GraphState):
    """
    Fetch the Valor 1000 ranking directly from the S3 JSON endpoint.
    Bypass HTML scraping for better performance and reliability.
    """

    print("--- NODE: Ranking Scraper ---")
    url = state["initial_url"]
    # For now, we use a placeholder. In the next step, we'll implement BeautifulSoup/Playwright.
    headers = {"User-Agent": "NuviaBot/1.0"}
    try:
        # 1. Fetch the data (DevOps Tip: Add headers to avoid 403 errors)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # 2. Use the Parser (Separation of Concerns)
        raw_json = response.json()

        # DEBUG: Check top-level keys
        print(f"DEBUG: Raw response preview: {response.text[:500]}")
        print(f"DEBUG: JSON Keys found: {list(raw_json.keys())}")
        
        # Verify the 'data' key exists and has items
        if "data" not in raw_json or not raw_json["data"]:
            print("WARNING: 'data' key is missing or empty in JSON.")
            return {"companies": [], "execution_logs": ["JSON 'data' key was empty."]}

        processed_data = parse_valor_1000_json(raw_json)

        if processed_data:
            for company in processed_data:
                if isinstance(company, dict):
                    company.setdefault("origin_company", True)
            print(f"DEBUG: Sample company payload: {processed_data[10]}")

        return {
            "company_queue": processed_data,
            "execution_logs": [f"Successfully scraped {len(processed_data)} companies."]
        }

    except Exception as e:
        return {"execution_logs": [f"Error during pulling data: {str(e)}"]}



def enrichment_node(state: GraphState):
    """
    Enrich company record with automated discovery.
    Log all search queries and results for transparency and debugging.
    """

    print("--- NODE: Enrichment ---")
    source_companies = state.get("companies", [])
    if len(source_companies) > _MAX_COMPANIES:
        print(
            f"[GUARD] enrichment_node received {len(source_companies)} companies; "
            f"trimming to {_MAX_COMPANIES}."
        )
        source_companies = source_companies[:_MAX_COMPANIES]
    enriched_companies = []
    processed_count = 0
    enrichment_logs = []
    llm_request_count = 0  # Track number of LLM API calls

    for index, company in enumerate(source_companies):
        company_copy = dict(company)
        company_name = company_copy.get("nome_empresa", "").strip()
        city = company_copy.get("sede", "").strip()
        node_label = "enrichment"

        if not company_name:
            # Skip entry because missing name blocks precise search
            log_company_event(
                company_copy,
                node_label,
                f"[SKIPPED] Company #{index+1}: Missing nome_empresa",
                execution_logs=enrichment_logs,
            )
            enriched_companies.append(company_copy)
            continue

        log_company_event(
            company_copy,
            node_label,
            f"[SEARCH #{index+1}] {company_name} ({city})",
            execution_logs=enrichment_logs,
        )
        log_company_event(
            company_copy,
            node_label,
            (
                f"Processing company #{index+1}: {company_name} | sede: {city} | setor: "
                f"{company_copy.get('setor', 'N/A')}"
            ),
            execution_logs=enrichment_logs,
            also_print=False,
        )

        site_query = get_search_query(company_name, city, "site")
        site_results = search_company_web_presence(site_query)
        log_company_event(company_copy, node_label, f"Official site Query: '{site_query}'", also_print=True)
        log_company_event(company_copy, node_label, f"Results: {len(site_results)} found", also_print=True)

        cnpj_query = get_search_query(company_name, city, "cnpj")
        cnpj_results = search_company_web_presence(cnpj_query)
        log_company_event(company_copy, node_label, f"CNPJ Query: '{cnpj_query}'", also_print=True)
        log_company_event(company_copy, node_label, f"Results: {len(cnpj_results)} found", also_print=True)

        linkedin_query = get_search_query(company_name, city, "linkedin")
        linkedin_results = search_company_web_presence(linkedin_query)
        log_company_event(company_copy, node_label, f"Linkedin Query: '{linkedin_query}'", also_print=True)
        log_company_event(company_copy, node_label, f"Results: {len(linkedin_results)} found", also_print=True)

        address_query = get_search_query(company_name, city, "address")
        address_results = search_company_web_presence(address_query)
        log_company_event(company_copy, node_label, f"Adress Query: '{address_query}'", also_print=True)
        log_company_event(company_copy, node_label, f"Results: {len(address_results)} found", also_print=True)

        about_query = f"{company_name} Sobre"
        about_results = search_company_web_presence(about_query)
        log_company_event(company_copy, node_label, f"About Query: '{about_query}'", also_print=True)
        log_company_event(company_copy, node_label, f"Results: {len(about_results)} found", also_print=True)

        should_retry_cnpj = not cnpj_results or not has_cnpj_in_results(cnpj_results)
        if should_retry_cnpj:
            cnpj_retry_queries = build_cnpj_retry_queries(company_name, city)
            cnpj_retry_results = []
            if cnpj_retry_queries:
                log_company_event(company_copy, node_label, "CNPJ Retry: Triggered", also_print=True)
                for retry_query in cnpj_retry_queries:
                    retry_results = search_company_web_presence(retry_query)
                    log_company_event(
                        company_copy,
                        node_label,
                        f"CNPJ Retry Query: '{retry_query}'",
                        also_print=True,
                    )
                    log_company_event(
                        company_copy,
                        node_label,
                        f"Results: {len(retry_results)} found",
                        also_print=True,
                    )
                    cnpj_retry_results = merge_search_results(cnpj_retry_results, retry_results)
                if cnpj_retry_results:
                    cnpj_results = merge_search_results(cnpj_results, cnpj_retry_results)
                log_company_event(
                    company_copy,
                    node_label,
                    f"[RETRY] CNPJ: {len(cnpj_retry_results)} result(s) from {len(cnpj_retry_queries)} query(ies)",
                    execution_logs=enrichment_logs,
                    also_print=False,
                )
            else:
                log_company_event(
                    company_copy,
                    node_label,
                    "[RETRY] CNPJ: Skipped (empty query list)",
                    execution_logs=enrichment_logs,
                    also_print=False,
                )

        
        evidence_lines = [
            f"Company: {company_name}",
            f"City: {city or 'Unknown'}",
            "Search results:",
        ]

        #Appending results from sites search
        if site_results:
            for rank, item in enumerate(site_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No official search evidence found.")

        #Appending results from cnpj search
        evidence_lines.append("CNPJ search results:")
        if cnpj_results:
            for rank, item in enumerate(cnpj_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No CNPJ search evidence found.")

        # Appending results from Linkedin research
        evidence_lines.append("Linkedin search results:")
        if linkedin_results:
            for rank, item in enumerate(linkedin_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No Linkedin search evidence found.")

        # Appending results from address research
        evidence_lines.append("Address search results:")
        if address_results:
            for rank, item in enumerate(address_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No address search evidence found.")

        # Appending results from about/institutional research
        evidence_lines.append("About/Institutional search results:")
        if about_results:
            for rank, item in enumerate(about_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No about/institutional search evidence found.")

        llm_prompt = "\n".join(evidence_lines)
        system_directive = (
            "You are a corporate intelligence analyst specializing in the Brazilian market. "
            "Your goal is to extract specific identifiers for a company based on categorized search evidence. "
            "The evidence is divided into blocks: 'Official', 'CNPJ', 'Linkedin', and 'Address'. "
            "Prioritize information found in its respective category, but cross-reference data if needed. "
            
            "Instructions for data extraction:\n"
            "1. official_website: Extract the most credible official URL. Avoid news articles or social media links here.\n"
            "2. linkedin_url: Find the direct link to the company's official LinkedIn profile.\n"
            "3. physical_address: Extract the most complete physical address found (street, number, city, state).\n"
            "4. primary_cnpj: Extract the full 14-digit Brazilian CNPJ. Clean it of any formatting (dots, slashes).\n"
            "5. radical_cnpj: This is the first 8 digits of the primary_cnpj. Extract it only if a valid CNPJ is found.\n"
            "6. about_page_url: Extract the specific URL that leads to the 'About Us', 'Quem Somos', 'sobre nos', 'História' or similar institutional page.\n"
            "7. institutional_description: Provide a brief institutional description if clearly stated in the evidence; otherwise return null.\n"

            "Rules:\n"
            "- If evidence for any field is missing or inconclusive, return null.\n"
            "- Do not hallucinate or guess data points.\n"
            "- Output must be strictly a single JSON object with the following keys: "
            "official_website (string/null), linkedin_url (string/null), physical_address (string/null), "
            "primary_cnpj (string/null), radical_cnpj (string/null), about_page_url (string/null), "
            "institutional_description (string/null).\n"
            "FINAL VALIDATION STEP: Before outputting the JSON, perform a silent self-check on the following:\n"
            "ID Length: Ensure primary_cnpj has exactly 14 digits and radical_cnpj has exactly 8 digits. Do not strip leading zeros.\n"
            "JSON Schema: Ensure the keys official_website, linkedin_url, about_page_url, and institutional_description are all present. Use null if data is missing.\n"
            "If you detect a formatting error, fix it before returning the final object. Your output must be STRICT JSON ONLY."
        )

        # Use LLM decision because snippet context prioritizes official domain over SEO noise
        try:
            enrichment_llm = get_enrichment_llm()
            llm_request_count += 1  # Increment counter
            log_company_event(
                company_copy,
                node_label,
                f"[LLM REQUEST #{llm_request_count}] Sending enrichment prompt...",
                also_print=True,
            )
            
            prompt_template_phase1 = ChatPromptTemplate.from_messages([
                ("system", system_directive),
                ("human", "{user_input}")
            ])
            chain = prompt_template_phase1 | enrichment_llm
            llm_response = chain.invoke({"user_input": llm_prompt})
            analysis_text = getattr(llm_response, "content", str(llm_response))
            # Validate that we received a non-empty response
            if not analysis_text or not analysis_text.strip():
                raise ValueError("LLM returned empty response")
            
            # Extract JSON from markdown code blocks if present
            if "```json" in analysis_text:
                # Extract content between ```json and ```
                json_start = analysis_text.find("```json") + 7
                json_end = analysis_text.find("```", json_start)
                if json_end != -1:
                    analysis_text = analysis_text[json_start:json_end].strip()
            elif "```" in analysis_text:
                # Extract content between ``` and ```
                json_start = analysis_text.find("```") + 3
                json_end = analysis_text.find("```", json_start)
                if json_end != -1:
                    analysis_text = analysis_text[json_start:json_end].strip()
            
            parsed_output = json.loads(analysis_text)
            log_company_event(company_copy, node_label, "LLM Analysis: Success", also_print=True)
        except json.JSONDecodeError as e:
            log_company_event(
                company_copy,
                node_label,
                f"LLM Analysis: JSON Parse Error - {str(e)}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
            log_company_event(
                company_copy,
                node_label,
                f"Raw response: {analysis_text[:200]}",
                execution_logs=None,
                also_print=False,
            )
            parsed_output = {}
            log_company_event(
                company_copy,
                node_label,
                f"JSON Error for {company_name}: Invalid JSON format",
                execution_logs=enrichment_logs,
                also_print=False,
            )
        except Exception as e:
            log_company_event(
                company_copy,
                node_label,
                f"LLM Analysis: Failed ({str(e)})",
                execution_logs=enrichment_logs,
                also_print=True,
            )
            parsed_output = {}
            log_company_event(
                company_copy,
                node_label,
                f"LLM Error for {company_name}: {str(e)}",
                execution_logs=enrichment_logs,
                also_print=False,
            )

        # Extract and validate official_website
        official_website = parsed_output.get("official_website")
        if isinstance(official_website, str) and official_website.strip():
            company_copy["official_website"] = official_website.strip()
            log_company_event(
                company_copy,
                node_label,
                f"✓ official_website: {official_website.strip()}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["official_website"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ official_website: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate linkedin_url
        linkedin_url = parsed_output.get("linkedin_url")
        if isinstance(linkedin_url, str) and linkedin_url.strip():
            company_copy["linkedin_url"] = linkedin_url.strip()
            log_company_event(
                company_copy,
                node_label,
                f"✓ linkedin_url: {linkedin_url.strip()}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["linkedin_url"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ linkedin_url: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate physical_address
        physical_address = parsed_output.get("physical_address")
        if isinstance(physical_address, str) and physical_address.strip():
            company_copy["physical_address"] = physical_address.strip()
            log_company_event(
                company_copy,
                node_label,
                f"✓ physical_address: {physical_address.strip()}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["physical_address"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ physical_address: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate primary_cnpj
        primary_cnpj = parsed_output.get("primary_cnpj")
        if isinstance(primary_cnpj, str) and primary_cnpj.strip():
            company_copy["primary_cnpj"] = primary_cnpj.strip()
            log_company_event(
                company_copy,
                node_label,
                f"✓ primary_cnpj: {primary_cnpj.strip()}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["primary_cnpj"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ primary_cnpj: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate radical_cnpj (must be exactly 8 digits)
        radical_cnpj = parsed_output.get("radical_cnpj")
        if isinstance(radical_cnpj, str) and radical_cnpj.strip():
            radical_cnpj_clean = radical_cnpj.strip()
            # Validate that radical_cnpj contains exactly 8 digits
            if radical_cnpj_clean.isdigit() and len(radical_cnpj_clean) == 8:
                company_copy["radical_cnpj"] = radical_cnpj_clean
                log_company_event(
                    company_copy,
                    node_label,
                    f"✓ radical_cnpj: {radical_cnpj_clean}",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            else:
                company_copy["radical_cnpj"] = None
                log_company_event(
                    company_copy,
                    node_label,
                    f"✗ radical_cnpj: Invalid format (expected 8 digits, got '{radical_cnpj_clean}')",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
        else:
            company_copy["radical_cnpj"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ radical_cnpj: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate about_page_url
        about_page_url = parsed_output.get("about_page_url")
        if isinstance(about_page_url, str) and about_page_url.strip():
            company_copy["about_page_url"] = about_page_url.strip()
            log_company_event(
                company_copy,
                node_label,
                f"✓ about_page_url: {about_page_url.strip()}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["about_page_url"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ about_page_url: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # Extract and validate institutional_description
        institutional_description = parsed_output.get("institutional_description")
        if isinstance(institutional_description, str) and institutional_description.strip():
            company_copy["institutional_description"] = institutional_description.strip()
            log_company_event(
                company_copy,
                node_label,
                "✓ institutional_description: Captured",
                execution_logs=enrichment_logs,
                also_print=True,
            )
        else:
            company_copy["institutional_description"] = None
            log_company_event(
                company_copy,
                node_label,
                "✗ institutional_description: Not determined",
                execution_logs=enrichment_logs,
                also_print=True,
            )

        # ===== CHECK IF COMPANY QUALIFIES FOR DEEP SEARCH (After Phase 1 extraction) =====
        # Criteria: High-value sectors (Holding, Petróleo, Finanças) or Revenue > 5000 million R$
        high_priority_keywords = ["Holding", "Petróleo", "Finanças"]
        sector = company_copy.get("setor", "").strip()
        revenue_str = company_copy.get("receita_liquida_milhoes", "0").strip()
        
        qualifies_for_deep_search = False
        try:
            # Parse Brazilian number format: 5.006,4 → 5006.4
            # (period = thousands separator, comma = decimal separator)
            revenue_float = float(revenue_str.replace(".", "").replace(",", ".")) if revenue_str else 0
            # Check if any high-priority keyword is in the sector (substring match)
            sector_match = any(keyword.lower() in sector.lower() for keyword in high_priority_keywords)
            qualifies_for_deep_search = (
                sector_match or revenue_float > 5000
            )
        except (ValueError, TypeError):
            sector_match = any(keyword.lower() in sector.lower() for keyword in high_priority_keywords)
            qualifies_for_deep_search = sector_match
        
        # ===== FETCH CORPORATE STRUCTURE IF QUALIFIED - USING SNIPER CSV DATA =====
        deep_search_content = None
        corporate_csv_evidence = None
        if qualifies_for_deep_search and company_copy.get("primary_cnpj"):
            primary_cnpj = company_copy.get("primary_cnpj")
            log_company_event(
                company_copy,
                node_label,
                f"[SNIPER] Filtering CSV data for CNPJ: {primary_cnpj}",
                execution_logs=enrichment_logs,
                also_print=True,
            )
            
            sniper_parts = []
            
            # Call shareholding sniper - Neo4j Knowledge Graph focused extraction
            shareholding_data, shareholding_rows = get_filtered_csv_data(primary_cnpj, "shareholding")
            if shareholding_data:
                sniper_parts.append(shareholding_data)
                log_company_event(
                    company_copy,
                    node_label,
                    "✓ Sniper: Data extracted from fre_cia_aberta_posicao_acionaria_2025.csv",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
                log_company_event(
                    company_copy,
                    node_label,
                    f"✓ Sniper: {shareholding_rows} ownership records found for this company",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            else:
                log_company_event(
                    company_copy,
                    node_label,
                    "✗ Sniper: No ownership records found in fre_cia_aberta_posicao_acionaria_2025.csv",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            
            # Call governance sniper
            governance_data, governance_rows = get_filtered_csv_data(primary_cnpj, "governance")
            if governance_data:
                sniper_parts.append(governance_data)
                log_company_event(
                    company_copy,
                    node_label,
                    f"✓ Sniper: {governance_rows} governance records found for this company",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            
            if sniper_parts:
                deep_search_content = "\n\n".join(sniper_parts)
                corporate_csv_evidence = deep_search_content
                log_company_event(
                    company_copy,
                    node_label,
                    f"[DEEP SEARCH] Using CSV Sniper Data - Retrieved {len(deep_search_content)} chars from official sources",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            else:
                log_company_event(
                    company_copy,
                    node_label,
                    f"✗ Sniper: No records found for CNPJ {primary_cnpj}",
                    execution_logs=enrichment_logs,
                    also_print=True,
                )
            
            # this function is not being called in the current flow, because it was implemented a deterministic algoritm code to handle the relationship extraction;
            if False and deep_search_content:
                print(f"  └─ [PHASE 2] Analyzing corporate structure data for Neo4j fields...")
                
                system_directive_phase2 = (
                    "You are a specialized Corporate Graph Architect. Your role is to transform raw business structure data "
                    "into structured JSON objects designed for Neo4j Knowledge Graph ingestion. Focus on extracting entities "
                    "and the specific relationships: :OWNS and :SUBSIDIARY_OF.\n\n"

                    "CORE DEFINITIONS:\n"
                    "1. [:OWNS]: Triggered for ANY shareholder listed in the data. This captures the flow of capital.\n"
                    "2. [:SUBSIDIARY_OF]: Triggered ONLY when the Acionista is a Company (PJ) AND (Acionista_Controlador='S' OR Percentage > 50%). "
                    "This captures the legal hierarchy.\n\n"

                    "DATA VARIABLES RECEIVED:\n"
                    "- CNPJ_Companhia (Target Company ID)\n"
                    "- Nome_Companhia (Target Company Name)\n"
                    "- Acionista (Shareholder Name)\n"
                    "- CPF_CNPJ_Acionista (Shareholder ID)\n"
                    "- Percentual_Total_Acoes_Circulacao (Ownership %)\n"
                    "- Acionista_Controlador (S/N)\n\n"

                    "EXTRACTION RULES:\n"
                    "- IDENTITIES: Use CPF_CNPJ_Acionista as the unique 'source_id' and CNPJ_Companhia as the 'target_id'.\n"
                    "- PERCENTAGES: Always include the 'percentage' property as a float.\n"
                    "- TYPES: Identify if the Acionista is a :Person (CPF) or a :Company (CNPJ) based on the ID length or format.\n"
                    "- RECURSION: If a company is independent (no 'S'), set corporate_group_notes to 'Independent company'.\n\n"

                    "OUTPUT FORMAT (STRICT JSON):\n"
                    "{{\n"
                    "  \"corporate_group_notes\": \"A concise summary derived from Acionista_Controlador and Percentual_Total_Acoes_Circulacao\",\n"
                    "  \"relationships\": [\n"
                    "    {{\n"
                    "      \"source_id\": \"CPF_CNPJ_Acionista\",\n"
                    "      \"source_name\": \"Acionista\",\n"
                    "      \"source_label\": \"'Person' if 11 digits, 'Company' if 14 digits\",\n"
                    "      \"target_id\": \"CNPJ_Companhia\",\n"
                    "      \"relationship_type\": \"OWNS\",\n"
                    "      \"properties\": {{\n"
                    "        \"percentage\": \"Percentual_Total_Acoes_Circulacao\",\n"
                    "        \"is_controller\": \"True if Acionista_Controlador == 'S', else False\"\n"
                    "      }}\n"
                    "    }},\n"
                    "    {{\n"
                    "      \"source_id\": \"CNPJ_Companhia\",\n"
                    "      \"target_id\": \"CPF_CNPJ_Acionista\",\n"
                    "      \"relationship_type\": \"SUBSIDIARY_OF\",\n"
                    "      \"properties\": {{\n"
                    "        \"percentage\": \"Percentual_Total_Acoes_Circulacao\"\n"
                    "      }}\n"
                    "    }}\n"
                    "  ]\n"
                    "}}\n\n"
                    
                    "RULES:\n"
                    "- DO NOT hallucinate. Use only the provided business structure data\n"
                    "- Preserve exact shareholder names for Neo4j node matching.\n"
                    "- If no relationships are found, return 'relationships': [].\n"
                    "FINAL VALIDATION STEP: Before outputting the JSON, perform a silent self-check on the following:\n"
                    "Relationship Logic: Verify that every object in the relationships array has both a source_id and a target_id.\n"
                    "If you detect a formatting error, fix it before returning the final object. Your output must be STRICT JSON ONLY."
                    )
                
                # ===== DEBUG: Log Phase 2 Prompt Content =====
                print(f"\n  ╔════════════════════════════════════════════════════════════════╗")
                print(f"  ║          [PHASE 2 DEBUG] Deep Search LLM Input                ║")
                print(f"  ╚════════════════════════════════════════════════════════════════╝")
                print(f"\n  [SYSTEM PROMPT]:")
                print(f"  ─────────────────────────────────────────────────────────────────")
                print(system_directive_phase2)
                print(f"\n  [USER CONTENT - CSV Data Being Analyzed]:")
                print(f"  ─────────────────────────────────────────────────────────────────")
                print(deep_search_content)
                print(f"  ─────────────────────────────────────────────────────────────────\n")
                
                try:
                    enrichment_llm = get_enrichment_llm()
                    llm_request_count += 1
                    print(f"  └─ [LLM REQUEST #{llm_request_count}] Analyzing CSV structure data...")
                    
                    prompt_template_phase2 = ChatPromptTemplate.from_messages([
                        ("system", system_directive_phase2),
                        ("human", "{user_input}")
                    ])
                    chain = prompt_template_phase2 | enrichment_llm
                    llm_response_phase2 = chain.invoke({"user_input": deep_search_content})
                    analysis_text_phase2 = getattr(llm_response_phase2, "content", str(llm_response_phase2))
                    
                    # ===== DEBUG: Log LLM Response =====
                    print(f"\n  [LLM RESPONSE - Raw]:")
                    print(f"  ─────────────────────────────────────────────────────────────────")
                    print(analysis_text_phase2)
                    print(f"  ─────────────────────────────────────────────────────────────────\n")
                    
                    if not analysis_text_phase2 or not analysis_text_phase2.strip():
                        raise ValueError("LLM returned empty response for deep search analysis")
                    
                    # Extract JSON from markdown code blocks if present
                    if "```json" in analysis_text_phase2:
                        json_start = analysis_text_phase2.find("```json") + 7
                        json_end = analysis_text_phase2.find("```", json_start)
                        if json_end != -1:
                            analysis_text_phase2 = analysis_text_phase2[json_start:json_end].strip()
                    elif "```" in analysis_text_phase2:
                        json_start = analysis_text_phase2.find("```") + 3
                        json_end = analysis_text_phase2.find("```", json_start)
                        if json_end != -1:
                            analysis_text_phase2 = analysis_text_phase2[json_start:json_end].strip()
                    
                    # ===== DEBUG: Log Extracted JSON =====
                    print(f"  [LLM RESPONSE - Extracted JSON]:")
                    print(f"  ─────────────────────────────────────────────────────────────────")
                    print(analysis_text_phase2)
                    print(f"  ─────────────────────────────────────────────────────────────────\n")
                    
                    parsed_output_phase2 = json.loads(analysis_text_phase2)
                    print(f"  └─ Phase 2 Analysis: Success")
                except json.JSONDecodeError as e:
                    print(f"  └─ Phase 2 Analysis: JSON Parse Error - {str(e)}")
                    parsed_output_phase2 = {}
                    enrichment_logs.append(f"   Phase 2 JSON Error for {company_name}: Invalid JSON format")
                except Exception as e:
                    print(f"  └─ Phase 2 Analysis: Failed ({str(e)})")
                    parsed_output_phase2 = {}
                    enrichment_logs.append(f"   Phase 2 LLM Error for {company_name}: {str(e)}")
                
                # Extract corporate_group_notes from Phase 2
                corporate_group_notes = parsed_output_phase2.get("corporate_group_notes")
                if isinstance(corporate_group_notes, str) and corporate_group_notes.strip():
                    company_copy["corporate_group_notes"] = corporate_group_notes.strip()
                    print(f"     ✓ corporate_group_notes: {corporate_group_notes.strip()}")
                    enrichment_logs.append(f"   ✓ corporate_group_notes: {corporate_group_notes.strip()}")
                else:
                    company_copy["corporate_group_notes"] = None
                    print(f"     ✗ corporate_group_notes: Not found")
                
                # Extract found_brands from Phase 2
                found_brands = parsed_output_phase2.get("found_brands", [])
                if isinstance(found_brands, list):
                    found_brands = [str(b).strip() for b in found_brands if isinstance(b, (str, int)) and str(b).strip()]
                    company_copy["found_brands"] = found_brands
                    if found_brands:
                        print(f"     ✓ found_brands: {found_brands}")
                        enrichment_logs.append(f"   ✓ found_brands: {found_brands}")
                    else:
                        print(f"     ✗ found_brands: Empty array (no brands found)")
                        enrichment_logs.append(f"   ✗ found_brands: Empty")
                else:
                    company_copy["found_brands"] = []
                    print(f"     ✗ found_brands: Invalid format")
                
                # Extract relationships from Phase 2
                relationships = parsed_output_phase2.get("relationships", [])
                if isinstance(relationships, list) and relationships:
                    company_copy["relationships"] = relationships
                    print(f"     ✓ relationships: {len(relationships)} relationships found")
                    print(f"\n  [RELATIONSHIPS EXTRACTED]:")
                    print(f"  ─────────────────────────────────────────────────────────────────")
                    print(json.dumps(relationships, indent=2, ensure_ascii=False))
                    print(f"  ─────────────────────────────────────────────────────────────────\n")
                    enrichment_logs.append(f"   ✓ relationships: {len(relationships)} relationships found")
                else:
                    company_copy["relationships"] = []
                    print(f"     ✗ relationships: No relationships found")
                    enrichment_logs.append(f"   ✗ relationships: Empty")

            owns_relationships, corporate_group_notes, _ = get_shareholding_owns_relationships(primary_cnpj)
            company_copy["corporate_group_notes"] = corporate_group_notes
            # Brand extraction was previously LLM-based; keep deterministic output.
            company_copy["found_brands"] = []
            company_copy["relationships"] = owns_relationships
            log_company_event(
                company_copy,
                node_label,
                f"[DETERMINISTIC] OWNS relationships: {len(owns_relationships)}",
                execution_logs=enrichment_logs,
                also_print=False,
            )
        else:
            # No deep search: set Neo4j fields to null/empty
            company_copy["corporate_group_notes"] = None
            company_copy["found_brands"] = []
            company_copy["relationships"] = []
        enriched_companies.append(company_copy)
        processed_count += 1

    log_message = (
        f"Enrichment node processed {processed_count} companies with LLM-guided selection."
    )
    llm_summary = f"LLM API requests in enrichment node: {llm_request_count}"
    deep_search_note = "CSV Sniper integration active for corporate structure enrichment"

    return {
        "companies": enriched_companies,
        "execution_logs": [log_message, llm_summary, deep_search_note] + enrichment_logs,
        "corporate_csv_evidence": corporate_csv_evidence,
        "llm_request_count": llm_request_count,
    }


async def institutional_scraping_node(state: GraphState):
    """
    Extract institutional content from about_page_url and convert to markdown.
    """

    print("--- NODE: Institutional Scraping ---")
    source_companies = state.get("companies", [])
    if len(source_companies) > _MAX_COMPANIES:
        print(
            f"[GUARD] institutional_scraping_node received {len(source_companies)} companies; "
            f"trimming to {_MAX_COMPANIES}."
        )
        source_companies = source_companies[:_MAX_COMPANIES]
    markdown_results = []
    scraping_logs = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        for index, company in enumerate(source_companies):
            company_name = company.get("nome_empresa", "Unknown")
            about_page_url = company.get("about_page_url")
            node_label = "institutional"

            if not about_page_url:
                markdown_results.append(None)
                log_company_event(
                    company,
                    node_label,
                    f"[SKIPPED] Company #{index+1}: Missing about_page_url",
                    execution_logs=scraping_logs,
                    also_print=True,
                )
                continue

            try:
                response = await page.goto(about_page_url, wait_until="domcontentloaded", timeout=10000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    await page.wait_for_timeout(5000)

                if response and response.status >= 400:
                    raise ValueError(f"HTTP {response.status}")

                raw_html = await page.content()
                soup = BeautifulSoup(raw_html, "html.parser")

                for tag in soup(["script", "style", "header", "footer", "nav", "noscript"]):
                    tag.decompose()

                body = soup.body or soup
                cleaned_html = str(body)

                markdown = html2text.html2text(cleaned_html)
                markdown = "\n".join(line.strip() for line in markdown.splitlines() if line.strip())

                if markdown:
                    markdown_results.append(markdown)
                    log_company_event(
                        company,
                        node_label,
                        f"✓ Institutional markdown captured for {company_name}",
                        execution_logs=scraping_logs,
                        also_print=True,
                    )
                else:
                    markdown_results.append(None)
                    log_company_event(
                        company,
                        node_label,
                        f"✗ Institutional markdown empty for {company_name}",
                        execution_logs=scraping_logs,
                        also_print=True,
                    )
            except Exception as e:
                markdown_results.append(None)
                log_company_event(
                    company,
                    node_label,
                    f"✗ Institutional scrape failed for {company_name}: {str(e)}",
                    execution_logs=scraping_logs,
                    also_print=True,
                )

        await context.close()
        await browser.close()

    return {
        "institutional_markdown": markdown_results,
        "execution_logs": scraping_logs,
    }
