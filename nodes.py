# Responsible for housing the individual functions (nodes) that perform specific tasks like scraping and analysis.
import json
import requests
from langchain_openai import ChatOpenAI

from state import GraphState
from utils.parser import parse_valor_1000_json
from utils.tools import get_search_query, search_company_web_presence

# Lazy-load LLM on first use to avoid initialization errors when API key is not set
_enrichment_llm = None

def get_enrichment_llm():
    """
    Instantiate the LLM only when first needed, avoiding import-time API key errors.
    Reuse the same instance across invocations for efficiency.
    """
    global _enrichment_llm
    if _enrichment_llm is None:
        _enrichment_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    return _enrichment_llm



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
            print(f"DEBUG: Sample company payload: {processed_data[10]}")

        return {
            "companies": processed_data,
            "execution_logs": [f"Successfully scraped {len(processed_data)} companies."]
        }

    except Exception as e:
        return {"execution_logs": [f"Error during pulling data: {str(e)}"]}



def enrichment_node(state: GraphState):
    """
    Enrich company record with automated discovery.
    """

    print("--- NODE: Enrichment ---")
    source_companies = state.get("companies", [])
    enriched_companies = []
    processed_count = 0

    for index, company in enumerate(source_companies):
        if index >= 5:
            # Expand to full set later so batching keeps throughput safe
            enriched_companies.append(company)
            continue

        company_copy = dict(company)
        company_name = company_copy.get("nome_empresa", "").strip()
        city = company_copy.get("sede", "").strip()

        if not company_name:
            # Skip entry because missing name blocks precise search
            enriched_companies.append(company_copy)
            continue

        official_query = get_search_query(company_name, city, "official")
        official_results = search_company_web_presence(official_query)

        cnpj_query = get_search_query(company_name, city, "cnpj")
        cnpj_results = search_company_web_presence(cnpj_query)

        evidence_lines = [
            f"Company: {company_name}",
            f"City: {city or 'Unknown'}",
            "Official search results:",
        ]

        if official_results:
            for rank, item in enumerate(official_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No official search evidence found.")

        evidence_lines.append("CNPJ search results:")

        if cnpj_results:
            for rank, item in enumerate(cnpj_results, start=1):
                evidence_lines.append(
                    f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
                )
        else:
            evidence_lines.append("No CNPJ search evidence found.")

        llm_prompt = "\n".join(evidence_lines)
        system_directive = (
            "You are a corporate intelligence analyst. "
            "Pick the most credible official website URL and primary Brazilian CNPJ based on evidence. "
            "If evidence is inconclusive, return null for that field. "
            "Identify any corporate group relationship or notable brand connection and include a short note. "
            "Return JSON with keys: official_website (string or null), primary_cnpj (string or null), "
            "corporate_group_notes (string or null), found_brands (array of string)."
        )

        # Use LLM decision because snippet context prioritizes official domain over SEO noise
        try:
            enrichment_llm = get_enrichment_llm()
            llm_response = enrichment_llm.invoke(
                [
                    {"role": "system", "content": system_directive},
                    {"role": "user", "content": llm_prompt},
                ]
            )
            analysis_text = getattr(llm_response, "content", str(llm_response))
            parsed_output = json.loads(analysis_text)
        except Exception:
            parsed_output = {}

        official_website = parsed_output.get("official_website")
        if isinstance(official_website, str) and official_website.strip():
            company_copy["official_website"] = official_website.strip()
        else:
            # Default to None so downstream stage treats signal as uncertain
            company_copy["official_website"] = None

        primary_cnpj = parsed_output.get("primary_cnpj")
        if isinstance(primary_cnpj, str) and primary_cnpj.strip():
            company_copy["primary_cnpj"] = primary_cnpj.strip()
        else:
            company_copy["primary_cnpj"] = None

        corporate_notes = parsed_output.get("corporate_group_notes")
        if isinstance(corporate_notes, str) and corporate_notes.strip():
            company_copy["corporate_group_notes"] = corporate_notes.strip()
        else:
            company_copy["corporate_group_notes"] = None

        found_brands = parsed_output.get("found_brands")
        if isinstance(found_brands, list):
            company_copy["found_brands"] = [
                str(brand).strip() for brand in found_brands if str(brand).strip()
            ]
        else:
            # Keep empty list so later collector can append safely
            company_copy["found_brands"] = company_copy.get("found_brands") or []

        enriched_companies.append(company_copy)
        processed_count += 1

    log_message = (
        f"Enrichment node processed {processed_count} companies with LLM-guided selection."
    )

    return {
        "companies": enriched_companies,
        "execution_logs": [log_message],
    }
