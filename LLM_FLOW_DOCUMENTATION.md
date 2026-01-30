# 📊 LLM Enrichment Prompt & Request Tracking

## 1️⃣ WHERE IS THE ENRICHMENT PROMPT STORED?

### Location in Code
**File:** `nodes.py` | **Function:** `enrichment_node()` | **Lines:** 139-142

```python
llm_prompt = "\n".join(evidence_lines)  # ← LINE 139: PROMPT IS STORED HERE
system_directive = (                    # ← LINE 140-142: SYSTEM INSTRUCTIONS
    "You are a corporate intelligence analyst. "
    "Pick the most credible official website URL and primary Brazilian CNPJ based on evidence. "
    "If evidence is inconclusive, return null for that field. "
    "Identify any corporate group relationship or notable brand connection and include a short note. "
    "Return JSON with keys: official_website (string or null), primary_cnpj (string or null), "
    "corporate_group_notes (string or null), found_brands (array of string)."
)
```

### Prompt Construction (Step-by-Step)

**Lines 111-138: Build evidence_lines list**
```python
evidence_lines = [
    f"Company: {company_name}",
    f"City: {city or 'Unknown'}",
    "Official search results:",
]

# Add Official search results (lines 116-122)
if official_results:
    for rank, item in enumerate(official_results, start=1):
        evidence_lines.append(
            f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
        )
else:
    evidence_lines.append("No official search evidence found.")

# Add CNPJ search results (lines 126-133)
evidence_lines.append("CNPJ search results:")
if cnpj_results:
    for rank, item in enumerate(cnpj_results, start=1):
        evidence_lines.append(
            f"{rank}. Title: {item.get('title', '')}\n   Link: {item.get('link', '')}\n   Snippet: {item.get('snippet', '')}"
        )
else:
    evidence_lines.append("No CNPJ search evidence found.")

# JOIN ALL INTO SINGLE STRING (line 139)
llm_prompt = "\n".join(evidence_lines)
```

---

## 2️⃣ EXAMPLE PROMPT FOR PETROBRAS

### What gets passed to LLM:

```
Company: Petrobras
City: RJ
Official search results:
1. Title: Petrobras - Exploração e Produção de Petróleo
   Link: https://www.petrobras.com.br
   Snippet: Petrobras is the largest oil company in Brazil...

2. Title: Petrobras Investor Relations
   Link: https://ri.petrobras.com.br
   Snippet: Financial information and investor news...

3. Title: Petrobras Careers
   Link: https://careers.petrobras.com.br
   Snippet: Join our team of 80,000+ employees...

4. Title: Petrobras Wikipedia
   Link: https://en.wikipedia.org/wiki/Petrobras
   Snippet: Petrobras is a state-owned oil company...

5. Title: Petrobras News - Latest Updates
   Link: https://news.petrobras.com.br
   Snippet: Press releases and company announcements...

CNPJ search results:
1. Title: Petrobras CNPJ - Receita Federal
   Link: https://receita.federal.gov.br/...
   Snippet: CNPJ: 33.000.167/0001-01 Petrobras S.A....

2. Title: Petrobras Company Registration
   Link: https://cnpj.biz/33000167000101
   Snippet: Legal entity information and details...

3. Title: Petrobras Tax Status
   Link: https://portal.srf.gov.br/...
   Snippet: Updated tax information for Petrobras...

4. Title: Petrobras Corporate Info
   Link: https://jucerja.gov.br/...
   Snippet: Company registration details...

5. Title: Petrobras Registry
   Link: https://cvm.gov.br/...
   Snippet: Securities Commission registration...
```

---

## 3️⃣ HOW MANY LLM REQUESTS ARE MADE?

### Current Configuration (First 5 Companies)

**Per Company:** **1 LLM request**
- Official results (5 results) → fed to LLM
- CNPJ results (5 results) → fed to LLM
- **1 combined prompt = 1 LLM call**

**Total with 5 companies:**
```
Companies 0-4: 5 companies × 1 LLM call each = 5 LLM requests
Companies 5-999: 0 LLM calls (skipped, stored as-is)
─────────────────────────────────────────────────────
TOTAL: 5 LLM API requests (for test run)
```

### At Full Scale (1000 Companies)
```
If you remove the hardcoded limit (index >= 5):
1000 companies × 1 LLM request each = 1000 LLM API calls
```

---

## 4️⃣ WHERE TO VERIFY LLM REQUESTS

### Terminal Output (Real-time)

When you run `python main.py`, you'll see:

```
--- NODE: Enrichment ---

[SEARCH #1] Petrobras (RJ)
  └─ Official Query: 'Petrobras RJ site oficial'
     Results: 5 found
  └─ CNPJ Query: 'Petrobras RJ CNPJ Receita Federal'
     Results: 5 found
  └─ [LLM REQUEST #1] Sending enrichment prompt...  ← REQUEST COUNTER
     ✓ Website: https://www.petrobras.com.br
     ✓ CNPJ: 08.841.475/0001-12

[SEARCH #2] JBS (SP)
  ...
  └─ [LLM REQUEST #2] Sending enrichment prompt...  ← REQUEST COUNTER

[SEARCH #3] Raízen (RJ)
  ...
  └─ [LLM REQUEST #3] Sending enrichment prompt...  ← REQUEST COUNTER

[SEARCH #4] Vale (RJ)
  ...
  └─ [LLM REQUEST #4] Sending enrichment prompt...  ← REQUEST COUNTER

[SEARCH #5] Vibra (RJ)
  ...
  └─ [LLM REQUEST #5] Sending enrichment prompt...  ← REQUEST COUNTER

--- Final Process Logs ---
  Successfully scraped 1000 companies.
  Enrichment node processed 5 companies with LLM-guided selection.
  Total LLM API requests: 5  ← SUMMARY COUNT
```

### Grep Command to Extract LLM Metrics

```bash
# Show all LLM requests
python main.py 2>&1 | grep "LLM REQUEST"

# Show just the final count
python main.py 2>&1 | grep "Total LLM API requests"

# Show requests + results
python main.py 2>&1 | grep -E "(LLM REQUEST|Website|CNPJ)"
```

---

## 5️⃣ PROMPT FLOW DIAGRAM

```
Company Data (nome_empresa, sede, setor)
     ↓
[Query Generator] → "company SP site oficial"
     ↓
[DuckDuckGo Search] → 5 results {title, link, snippet}
     ↓
[Query Generator] → "company SP CNPJ Receita Federal"
     ↓
[DuckDuckGo Search] → 5 results {title, link, snippet}
     ↓
[Evidence Formatter] → Build evidence_lines list
     ↓
[Prompt Assembly] → llm_prompt = "\n".join(evidence_lines)  ← STORED HERE
     ↓
[LLM Invocation] → llm_response = enrichment_llm.invoke([
                     {"role": "system", "content": system_directive},
                     {"role": "user", "content": llm_prompt}
                   ])
     ↓
[Response Parser] → Extract JSON from markdown
     ↓
[Storage] → Add official_website, primary_cnpj, corporate_group_notes, found_brands to company dict
```

---

## 6️⃣ LLM REQUEST COUNTER CODE LOCATION

**File:** `nodes.py` | **Lines:** 69, 147-148, 217-218

```python
# Line 69: Initialize counter
llm_request_count = 0

# Lines 147-148: Increment counter before API call
llm_request_count += 1
print(f"  └─ [LLM REQUEST #{llm_request_count}] Sending enrichment prompt...")

# Lines 217-218: Log final count
llm_summary = f"Total LLM API requests: {llm_request_count}"
return {
    "companies": enriched_companies,
    "execution_logs": [log_message, llm_summary] + enrichment_logs,
}
```

---

## 📈 Cost Implications

### Token Usage Per LLM Request
Estimated tokens per company enrichment:

```
Prompt Input:
  - Company data: ~50 tokens
  - 5 official results (title+link+snippet): ~400 tokens
  - 5 CNPJ results (title+link+snippet): ~400 tokens
  - System directive: ~100 tokens
  ────────────────────────────
  Total Input: ~950 tokens per request

Response Output:
  - JSON with website, CNPJ, corporate notes, brands: ~200 tokens
  ────────────────────────────
  Total Output: ~200 tokens per request

TOTAL PER REQUEST: ~1,150 tokens
```

### At Different Scales

| Companies | LLM Requests | Estimated Tokens | Approx Cost (GPT-4o-mini) |
|-----------|--------------|------------------|---------------------------|
| 5 (Test)  | 5            | 5,750             | ~$0.02                   |
| 100       | 100          | 115,000           | ~$0.35                   |
| 1,000     | 1,000        | 1,150,000         | ~$3.50                   |
| 10,000    | 10,000       | 11,500,000        | ~$35.00                  |

---

## 🎯 Summary

✅ **Prompt Location:** `nodes.py:139` - `llm_prompt = "\n".join(evidence_lines)`

✅ **LLM Requests per Company:** 1

✅ **Current Test Total:** 5 LLM requests

✅ **Tracking Method:** Terminal output shows `[LLM REQUEST #N]` for each call

✅ **Final Count:** Shown in `"Total LLM API requests: X"` log at end
