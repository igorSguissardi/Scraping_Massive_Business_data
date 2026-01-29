# utils/parsers.py
import re

def parse_valor_1000_json(json_data: dict):
    """
    Return list of company dictionaries parsed from Valor 1000 JSON payload.
    """

    companies = []
    rows_source = json_data.get("data") or json_data.get("aaData") or []

    row_iterable = rows_source.values() if isinstance(rows_source, dict) else rows_source

    for raw_row in row_iterable:
        if isinstance(raw_row, list):
            # Flatten single-element list because the export wraps each row string
            row_content = raw_row[0] if len(raw_row) == 1 and isinstance(raw_row[0], str) else raw_row
        else:
            row_content = raw_row

        if isinstance(row_content, str):
            normalized_row = row_content.replace("\ufeff", "").strip()
            parts = [part.strip() for part in normalized_row.split(";")]
        elif isinstance(row_content, list):
            # Preserve supplied column split if the upstream API already parsed it
            parts = [str(part).strip() for part in row_content]
        else:
            continue

        if len(parts) < 8:
            continue

        clean_name = re.sub(r"<[^>]*>", "", parts[2]).strip()
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