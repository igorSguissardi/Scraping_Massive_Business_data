# SUMMARY OF CODE CHANGES

## Changes Made

### 1. **state.py** - Added Cost Tracking
**What changed:**
```python
# Added to GraphState class:
llm_request_count: int
```

**Why:** To track total LLM API calls for cost auditing and rate limit monitoring.

---

### 2. **requirements.txt** - Added HTML Parser
**What changed:**
```
+ beautifulsoup4==4.12.2
```

**Why:** Required to parse HTML tables and divs from cnpj.biz when fetching corporate structure data.

---

### 3. **utils/tools.py** - Added Deep Search Function
**What changed:**
```python
# Added imports:
import asyncio
import requests
from bs4 import BeautifulSoup
from typing import Optional

# Added new async function:
async def fetch_corporate_structure(cnpj: str) -> Optional[str]:
    """
    Fetch QSA/Sócio/Acionistas data from cnpj.biz
    - Constructs URL: https://cnpj.biz/{clean_cnpj}
    - Uses browser-like User-Agent
    - Parses HTML with BeautifulSoup
    - Searches for: "Sócio", "Acionistas", "QSA", "Sócio-Gerente"
    - Returns text (max 3000 chars) or None
    - Handles timeouts and connection errors gracefully
    """
```

**Why:** To scrape corporate ownership structures (QSA/shareholders) from cnpj.biz for high-value companies.

---

### 4. **nodes.py** - Complete Async Refactor
**What changed:**

#### A) Added imports:
```python
import asyncio
from utils.tools import fetch_corporate_structure
```

#### B) Restructured enrichment_node:
```python
def enrichment_node(state: GraphState):
    """Wrapper - now calls async pipeline"""
    enriched_companies, new_llm_count, enrichment_logs = asyncio.run(
        _async_enrichment_pipeline(source_companies, llm_request_count)
    )
    return {
        "companies": enriched_companies,
        "execution_logs": enrichment_logs,
        "llm_request_count": new_llm_count,
    }
```

#### C) Added new async function - Orchestrator:
```python
async def _async_enrichment_pipeline(companies: list, initial_llm_count: int):
    """
    Parallel processing with Semaphore(5) for rate limiting
    - Uses asyncio.gather() for concurrent processing
    - Max 5 concurrent LLM requests at a time
    - Prevents API throttling (429 errors)
    """
```

#### D) Added new async function - Single Company Processor:
```python
async def _enrich_single_company(index: int, company: dict, llm_request_count: int):
    """
    Enriches single company:
    1. Web searches (site, CNPJ, LinkedIn, address)
    2. Decision logic: Check if high-value
       IF (sector in ["Holding", "Petróleo", "Finanças"]) 
          OR (revenue > 5000M)
       THEN qualifies_for_deep_search = True
    3. Deep search: IF qualified, AWAIT fetch_corporate_structure()
    4. LLM enrichment: Extract structured data
    5. Neo4j fields: Extract corporate_group_notes + found_brands
    """
```

#### E) Enhanced LLM System Directive:
```python
system_directive = """
You are a corporate intelligence analyst specializing in Brazilian market 
graph analysis. Your goal is to extract specific identifiers and ownership 
relationships for Neo4j Knowledge Graph construction.

GRAPH-ORIENTED DATA EXTRACTION:
1. official_website: Extract credible official URL
2. linkedin_url: Find direct link to LinkedIn profile
3. primary_cnpj: Extract 14-digit CNPJ
4. radical_cnpj: First 8 digits of CNPJ
5. corporate_group_notes: CRITICAL FOR NEO4J - Summarize ownership structure
   Format: "Owned by Group X via Holding Y"
   Only populate if DEEP SEARCH data exists
6. found_brands: List subsidiary brands as array
   Example: ['BrandA', 'BrandB']
   Extract from deep search societary structure
"""
```

#### F) Added Deep Search Logic:
```python
high_priority_sectors = ["Holding", "Petróleo", "Finanças"]
qualifies_for_deep_search = (
    sector in high_priority_sectors or revenue_float > 5000
)

if qualifies_for_deep_search:
    deep_search_content = await fetch_corporate_structure(primary_cnpj)
    if deep_search_content:
        evidence_lines.append("### DEEP SEARCH: SOCIETARY STRUCTURE ###")
        evidence_lines.append(deep_search_content)
```

#### G) Added Neo4j-Ready Field Extraction:
```python
# Extract corporate_group_notes (ownership structure)
corporate_group_notes = parsed_output.get("corporate_group_notes")

# Extract found_brands (subsidiaries array)
found_brands = parsed_output.get("found_brands", [])
```

**Why:** 
- Enable parallel processing without API rate limiting
- Fetch corporate structure for strategic companies only
- Extract Neo4j-ready data (ownership relationships and subsidiaries)
- Track costs via llm_request_count

---

## Summary

**Total Lines Changed:** ~800 lines

**Files Modified:** 4
- state.py: 1 line added (llm_request_count field)
- requirements.txt: 1 line added (beautifulsoup4)
- utils/tools.py: ~80 lines added (fetch_corporate_structure function)
- nodes.py: ~720 lines refactored (async architecture + deep search + Neo4j fields)

**Key Capabilities Added:**
1. ✅ Async parallel processing (up to 5 concurrent LLM requests)
2. ✅ Deep corporate structure scraping from cnpj.biz
3. ✅ Decision logic: triggers deep search for Holding/Petróleo/Finanças or Revenue > $5B
4. ✅ Neo4j-ready fields: corporate_group_notes (ownership) + found_brands (subsidiaries)
5. ✅ Rate limiting to prevent API throttling
6. ✅ Cost tracking via llm_request_count
7. ✅ Comprehensive error handling (timeouts, connection errors, LLM failures)

**Backward Compatibility:** 100% - No breaking changes, all existing functionality preserved.
