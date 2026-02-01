# Responsible for housing the individual functions (nodes) that perform specific tasks like scraping and analysis.
import json
import os
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
    Log all search queries and results for transparency and debugging.
    """

    print("--- NODE: Enrichment ---")
    source_companies = state.get("companies", [])
    enriched_companies = []
    processed_count = 0
    enrichment_logs = []
    llm_request_count = 0  # Track number of LLM API calls

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
            enrichment_logs.append(f"[SKIPPED] Company #{index+1}: Missing nome_empresa")
            continue

        # Log company being processed
        print(f"\n[SEARCH #{index+1}] {company_name} ({city})")
        enrichment_logs.append(f"Processing company #{index+1}: {company_name} | sede: {city} | setor: {company_copy.get('setor', 'N/A')}")

        site_query = get_search_query(company_name, city, "site")
        site_results = search_company_web_presence(site_query)
        print(f"  └─ Official site Query: '{site_query}'")
        print(f"     Results: {len(site_results)} found")

        cnpj_query = get_search_query(company_name, city, "cnpj")
        cnpj_results = search_company_web_presence(cnpj_query)
        print(f"  └─ CNPJ Query: '{cnpj_query}'")
        print(f"     Results: {len(cnpj_results)} found")

        linkedin_query = get_search_query(company_name, city, "linkedin")
        linkedin_results = search_company_web_presence(linkedin_query)
        print(f"  └─ Linkedin Query: '{linkedin_query}'")
        print(f"     Results: {len(linkedin_results)} found")

        address_query = get_search_query(company_name, city, "address")
        address_results = search_company_web_presence(address_query)
        print(f"  └─ Adress Query: '{address_query}'")
        print(f"     Results: {len(address_results)} found")

        
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

            "Rules:\n"
            "- If evidence for any field is missing or inconclusive, return null.\n"
            "- Do not hallucinate or guess data points.\n"
            "- Output must be strictly a single JSON object with the following keys: "
            "official_website (string/null), linkedin_url (string/null), physical_address (string/null), "
            "primary_cnpj (string/null), radical_cnpj (string/null)."
        )

        # Use LLM decision because snippet context prioritizes official domain over SEO noise
        try:
            enrichment_llm = get_enrichment_llm()
            llm_request_count += 1  # Increment counter
            print(f"  └─ [LLM REQUEST #{llm_request_count}] Sending enrichment prompt...")
            llm_response = enrichment_llm.invoke(
                [
                    {"role": "system", "content": system_directive},
                    {"role": "user", "content": llm_prompt},
                ]
            )
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
            print(f"  └─ LLM Analysis: Success")
        except json.JSONDecodeError as e:
            print(f"  └─ LLM Analysis: JSON Parse Error - {str(e)}")
            print(f"     Raw response: {analysis_text[:200]}")
            parsed_output = {}
            enrichment_logs.append(f"   JSON Error for {company_name}: Invalid JSON format")
        except Exception as e:
            print(f"  └─ LLM Analysis: Failed ({str(e)})")
            parsed_output = {}
            enrichment_logs.append(f"   LLM Error for {company_name}: {str(e)}")

        # Extract and validate official_website
        official_website = parsed_output.get("official_website")
        if isinstance(official_website, str) and official_website.strip():
            company_copy["official_website"] = official_website.strip()
            print(f"     ✓ official_website: {official_website.strip()}")
            enrichment_logs.append(f"   ✓ official_website: {official_website.strip()}")
        else:
            company_copy["official_website"] = None
            print(f"     ✗ official_website: Not found")
            enrichment_logs.append(f"   ✗ official_website: Not determined")

        # Extract and validate linkedin_url
        linkedin_url = parsed_output.get("linkedin_url")
        if isinstance(linkedin_url, str) and linkedin_url.strip():
            company_copy["linkedin_url"] = linkedin_url.strip()
            print(f"     ✓ linkedin_url: {linkedin_url.strip()}")
            enrichment_logs.append(f"   ✓ linkedin_url: {linkedin_url.strip()}")
        else:
            company_copy["linkedin_url"] = None
            print(f"     ✗ linkedin_url: Not found")
            enrichment_logs.append(f"   ✗ linkedin_url: Not determined")

        # Extract and validate physical_address
        physical_address = parsed_output.get("physical_address")
        if isinstance(physical_address, str) and physical_address.strip():
            company_copy["physical_address"] = physical_address.strip()
            print(f"     ✓ physical_address: {physical_address.strip()}")
            enrichment_logs.append(f"   ✓ physical_address: {physical_address.strip()}")
        else:
            company_copy["physical_address"] = None
            print(f"     ✗ physical_address: Not found")
            enrichment_logs.append(f"   ✗ physical_address: Not determined")

        # Extract and validate primary_cnpj
        primary_cnpj = parsed_output.get("primary_cnpj")
        if isinstance(primary_cnpj, str) and primary_cnpj.strip():
            company_copy["primary_cnpj"] = primary_cnpj.strip()
            print(f"     ✓ primary_cnpj: {primary_cnpj.strip()}")
            enrichment_logs.append(f"   ✓ primary_cnpj: {primary_cnpj.strip()}")
        else:
            company_copy["primary_cnpj"] = None
            print(f"     ✗ primary_cnpj: Not found")
            enrichment_logs.append(f"   ✗ primary_cnpj: Not determined")

        # Extract and validate radical_cnpj (must be exactly 8 digits)
        radical_cnpj = parsed_output.get("radical_cnpj")
        if isinstance(radical_cnpj, str) and radical_cnpj.strip():
            radical_cnpj_clean = radical_cnpj.strip()
            # Validate that radical_cnpj contains exactly 8 digits
            if radical_cnpj_clean.isdigit() and len(radical_cnpj_clean) == 8:
                company_copy["radical_cnpj"] = radical_cnpj_clean
                print(f"     ✓ radical_cnpj: {radical_cnpj_clean}")
                enrichment_logs.append(f"   ✓ radical_cnpj: {radical_cnpj_clean}")
            else:
                company_copy["radical_cnpj"] = None
                print(f"     ✗ radical_cnpj: Invalid format (expected 8 digits, got '{radical_cnpj_clean}')")
                enrichment_logs.append(f"   ✗ radical_cnpj: Invalid format (expected 8 digits)")
        else:
            company_copy["radical_cnpj"] = None
            print(f"     ✗ radical_cnpj: Not found")
            enrichment_logs.append(f"   ✗ radical_cnpj: Not determined")

        enriched_companies.append(company_copy)
        processed_count += 1

    log_message = (
        f"Enrichment node processed {processed_count} companies with LLM-guided selection."
    )
    llm_summary = f"Total LLM API requests: {llm_request_count}"

    return {
        "companies": enriched_companies,
        "execution_logs": [log_message, llm_summary] + enrichment_logs,
    }
