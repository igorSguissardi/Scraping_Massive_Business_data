# Responsible for housing the individual functions (nodes) that perform specific tasks like scraping and analysis.
import asyncio
import json
import os
import requests
from langchain_openai import ChatOpenAI

from state import GraphState
from utils.parser import parse_valor_1000_json
from utils.tools import get_search_query, search_company_web_presence, get_mock_corporate_data, get_filtered_csv_data

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
        if index >= 2:
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
            print(f"  └─ [SNIPER] Filtering CSV data for CNPJ: {primary_cnpj}")
            
            sniper_parts = []
            
            # Call shareholding sniper - Neo4j Knowledge Graph focused extraction
            shareholding_data, shareholding_rows = get_filtered_csv_data(primary_cnpj, "shareholding")
            if shareholding_data:
                sniper_parts.append(shareholding_data)
                enrichment_logs.append(f"   ✓ Sniper: Data extracted from fre_cia_aberta_posicao_acionaria_2025.csv")
                enrichment_logs.append(f"   ✓ Sniper: {shareholding_rows} ownership records found for this company")
                print(f"     ✓ Sniper: Data extracted from fre_cia_aberta_posicao_acionaria_2025.csv")
                print(f"     ✓ Sniper: {shareholding_rows} ownership records found for this company")
            else:
                enrichment_logs.append(f"   ✗ Sniper: No ownership records found in fre_cia_aberta_posicao_acionaria_2025.csv")
            
            # Call governance sniper
            governance_data, governance_rows = get_filtered_csv_data(primary_cnpj, "governance")
            if governance_data:
                sniper_parts.append(governance_data)
                enrichment_logs.append(f"   ✓ Sniper: {governance_rows} governance records found for this company")
                print(f"     ✓ Sniper: {governance_rows} governance records found for this company")
            
            if sniper_parts:
                deep_search_content = "\n\n".join(sniper_parts)
                corporate_csv_evidence = deep_search_content
                print(f"  └─ [DEEP SEARCH] Using CSV Sniper Data - Retrieved {len(deep_search_content)} chars from official sources")
            else:
                enrichment_logs.append(f"   ✗ Sniper: No records found for CNPJ {primary_cnpj}")
            
            if deep_search_content:
                print(f"  └─ [PHASE 2] Analyzing corporate structure data for Neo4j fields...")
                
                system_directive_phase2 = (
                    "You are a corporate structure analyst specializing in Brazilian company ownership hierarchies. "
                    "Your task is to analyze official FRE shareholding data from CVM (Brazil's Securities Commission) "
                    "and extract information about corporate groups, beneficial owners, and control structures for Neo4j Knowledge Graph construction.\n\n"
                    
                    "DATA SOURCE:\n"
                    "Official CVM filing data (fre_cia_aberta_posicao_acionaria_2025.csv) containing:\n"
                    "- CNPJ_Companhia: Target company unique identifier\n"
                    "- Nome_Companhia: Company name\n"
                    "- Acionista: Shareholder name (Person or Company)\n"
                    "- CPF_CNPJ_Acionista: Shareholder's unique identifier\n"
                    "- Percentual_Total_Acoes_Circulacao: Ownership percentage\n"
                    "- Acionista_Controlador: Binary (S/N) indicating controlling shareholder\n"
                    "- Participante_Acordo_Acionistas: Binary (S/N) indicating voting agreement participation\n\n"
                    
                    "EXTRACTION INSTRUCTIONS FOR NEO4J KNOWLEDGE GRAPH:\n"
                    "1. corporate_group_notes (string): Identify the ultimate beneficial owner(s) and control structure:\n"
                    "   - If Acionista_Controlador='S' exists, name the controlling shareholder with percentage\n"
                    "   - If multiple controlling shareholders, list hierarchy\n"
                    "   - If independent (no controlling shareholder), return 'Independent company - no controlling shareholder'\n"
                    "   - Keep it concise: max 150 characters\n"
                    "   Examples:\n"
                    "   - 'Controlled by [Name] with [X]%' (if single controller)\n"
                    "   - 'Jointly controlled by [Name A] and [Name B]' (if multiple)\n"
                    "   - 'Independent company - no controlling shareholder' (if none)\n\n"
                    "2. found_brands (array of strings): Extract distinct shareholder entities (potential subsidiary/brand nodes):\n"
                    "   - List all unique Acionista names with Acionista_Controlador='S' or high percentages (>10%)\n"
                    "   - These become nodes in the ownership graph\n"
                    "   - Return as JSON array: ['Entity1', 'Entity2', 'Entity3']\n"
                    "   - If no controlling shareholders or significant stakes, return empty array []\n\n"
                    
                    "OUTPUT FORMAT:\n"
                    "Return STRICTLY a single JSON object:\n"
                    "{\n"
                    "  \"corporate_group_notes\": \"string or null\",\n"
                    "  \"found_brands\": [\"array\", \"of\", \"shareholder\", \"entities\"]\n"
                    "}\n\n"
                    
                    "RULES:\n"
                    "- Extract ONLY what is explicitly stated in the CVM data. No inference or hallucination.\n"
                    "- For corporate_group_notes: Focus on Acionista_Controlador='S' entries\n"
                    "- For found_brands: Include shareholders with S='S' (controlling) or percentages > 10%\n"
                    "- Preserve exact spelling of shareholder names for Neo4j node matching\n"
                    "- If no controlling shareholder found, mark as 'Independent company - no controlling shareholder'"
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
                    llm_response_phase2 = enrichment_llm.invoke(
                        [
                            {"role": "system", "content": system_directive_phase2},
                            {"role": "user", "content": deep_search_content},
                        ]
                    )
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
        else:
            # No deep search: set Neo4j fields to null/empty
            company_copy["corporate_group_notes"] = None
            company_copy["found_brands"] = []

        enriched_companies.append(company_copy)
        processed_count += 1

    log_message = (
        f"Enrichment node processed {processed_count} companies with LLM-guided selection."
    )
    llm_summary = f"Total LLM API requests: {llm_request_count}"
    deep_search_note = "CSV Sniper integration active for corporate structure enrichment"

    return {
        "companies": enriched_companies,
        "execution_logs": [log_message, llm_summary, deep_search_note] + enrichment_logs,
        "corporate_csv_evidence": corporate_csv_evidence,
    }
