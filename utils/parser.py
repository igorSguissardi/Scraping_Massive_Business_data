# utils/parsers.py
import re

def parse_valor_1000_json(json_data: dict):
    """
    Takes raw HTML and returns a list of company dictionaries.
    Verbal flexion: Imperative.
    """

    companies = []
    
    # Logic to find the table or list in the Valor 1000 page
    # Note: You'll need to inspect the site     to get the exact class names
    # The 'data' key contains the list of company strings found in your screenshot
    rows = json_data.get("data") or json_data.get("aaData") or []
    
    for row in rows:
        # Ensure row is a list before splitting; some APIs return lists of lists directly
        if isinstance(row, str):
            parts = row.split(";")
        else:
            parts = row # Already a list

        if len(parts) < 6: continue # Skip malformed rows
        # 2. Clean HTML tags (like <i class="tooltipster">...</i>)
        # Regex explanation: <[^>]*> matches anything between brackets
        clean_name = re.sub(r'<[^>]*>', '', parts[2]).strip()
        company = {
            "classificacao_2024": parts[0],
            "classificacao_2023": parts[1],
            "nome_empresa": clean_name,
            "sede": parts[3],
            "setor": parts[4],
            "receita_liquida_milhoes": parts[5],
            "lucro_liquido_milhoes": parts[7],
            "razao_social": parts[22].strip() if len(parts) > 22 else None
        }
        companies.append(company)
        
    return companies