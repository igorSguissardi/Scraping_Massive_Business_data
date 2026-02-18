"""Utility helpers for search-driven enrichment."""

import asyncio
import os
import random
import re
import threading
import time
import zipfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from ddgs import DDGS


# ============================================================================
# CNPJ SNIPER - CSV-Based Corporate Structure Enrichment
# ============================================================================

_FRE_ZIP_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_2025.zip"
_FRE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "../data/fre_cia_aberta_2025"))
_FRE_ZIP_PATH = os.path.join(_FRE_DIR, "fre_cia_aberta_2025.zip")
_FRE_LOCK = threading.Lock()
_LOG_LOCK = threading.Lock()
_LOG_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "../logs"))


def clear_run_logs() -> None:
    """
    Clear log directory for new run.
    """
    os.makedirs(_LOG_DIR, exist_ok=True)
    removed_count = 0
    for entry in os.listdir(_LOG_DIR):
        path = os.path.join(_LOG_DIR, entry)
        try:
            if os.path.isfile(path):
                os.remove(path)
                removed_count += 1
        except OSError:
            continue
    print(f"[LOG] Cleared {removed_count} log file(s) from log directory.")


def _is_valid_zip(path: str) -> bool:
    """Return True when the ZIP file exists and passes integrity checks."""
    if not os.path.exists(path):
        return False
    try:
        with zipfile.ZipFile(path, "r") as zip_file:
            bad_file = zip_file.testzip()
            if bad_file is not None:
                print(f"[ERROR] Invalid ZIP: {path} (bad file: {bad_file})")
                return False
    except zipfile.BadZipFile as exc:
        print(f"[ERROR] Invalid ZIP: {path} ({exc})")
        return False
    except Exception as exc:
        print(f"[ERROR] Failed to read ZIP: {path} ({exc})")
        return False
    return True


def _download_fre_zip() -> None:
    """Download the FRE ZIP to a temp file, validate it, and move into place."""
    os.makedirs(_FRE_DIR, exist_ok=True)
    temp_path = f"{_FRE_ZIP_PATH}.part"
    if os.path.exists(temp_path):
        try:
            os.remove(temp_path)
        except OSError:
            pass
    print(f"[CACHE] Downloading FRE ZIP from CVM: {_FRE_ZIP_URL}")
    with requests.get(_FRE_ZIP_URL, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    if not _is_valid_zip(temp_path):
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise ValueError("Downloaded FRE ZIP is invalid.")
    os.replace(temp_path, _FRE_ZIP_PATH)
    print(f"[CACHE] Downloaded FRE ZIP to {_FRE_ZIP_PATH}")


def _ensure_fre_csv(file_name: str) -> str:
    """
    Ensure the requested FRE CSV exists locally.

    Downloads and extracts from CVM if missing.
    Returns the absolute path to the CSV.
    """
    os.makedirs(_FRE_DIR, exist_ok=True)
    csv_path = os.path.join(_FRE_DIR, file_name)

    if os.path.exists(csv_path):
        if os.path.getsize(csv_path) > 0:
            return csv_path
        try:
            os.remove(csv_path)
        except OSError:
            pass

    with _FRE_LOCK:
        if os.path.exists(csv_path):
            if os.path.getsize(csv_path) > 0:
                return csv_path
            try:
                os.remove(csv_path)
            except OSError:
                pass

        if not _is_valid_zip(_FRE_ZIP_PATH):
            if os.path.exists(_FRE_ZIP_PATH):
                try:
                    os.remove(_FRE_ZIP_PATH)
                except OSError:
                    pass
            _download_fre_zip()

        with zipfile.ZipFile(_FRE_ZIP_PATH, "r") as zip_file:
            candidate = None
            for name in zip_file.namelist():
                if name.endswith(file_name):
                    candidate = name
                    break
            if candidate is None:
                raise FileNotFoundError(f"CSV '{file_name}' not found in {_FRE_ZIP_PATH}")
            zip_file.extract(candidate, _FRE_DIR)

            extracted_path = os.path.join(_FRE_DIR, candidate)
            if extracted_path != csv_path:
                os.replace(extracted_path, csv_path)
            if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
                raise FileNotFoundError(f"CSV '{file_name}' extraction failed: {csv_path}")

    return csv_path

def clean_cnpj(cnpj_str: str) -> str:
    """
    Normalize a CNPJ string by removing all non-numeric characters.
    
    Args:
        cnpj_str: CNPJ string with or without formatting (e.g., '00.000.000/0001-91' or '00000000000191')
        
    Returns:
        Numeric-only CNPJ string (e.g., '00000000000191')
    """
    if not cnpj_str:
        return ""
    return re.sub(r'\D', '', str(cnpj_str))


# Singleton for CSV caching - prevents repeated disk reads during parallel execution
class _CSVCache:
    """Thread-safe singleton for in-memory CSV caching."""
    _instance = None
    _dataframes = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_CSVCache, cls).__new__(cls)
        return cls._instance
    
    def get_shareholding(self) -> Optional[pd.DataFrame]:
        """Load and cache the shareholding CSV (posicao_acionaria)."""
        key = "shareholding"
        if key not in self._dataframes:
            try:
                path = _ensure_fre_csv("fre_cia_aberta_posicao_acionaria_2025.csv")
                # Load with proper encoding and separator (ISO-8859-1 for Brazilian CSV files)
                # Use converters to preserve leading zeros in identity columns.
                df = pd.read_csv(
                    path,
                    sep=";",
                    encoding="latin1",
                    converters={
                        "CNPJ_Companhia": str,
                        "CPF_CNPJ_Acionista": str,
                    },
                )
                self._dataframes[key] = df
                print(f"[CACHE] Loaded shareholding CSV (fre_cia_aberta_posicao_acionaria_2025.csv): {len(df)} rows")
            except Exception as e:
                print(f"[ERROR] Failed to load shareholding CSV: {e}")
                self._dataframes[key] = None
        return self._dataframes[key]
    
    def get_governance(self) -> Optional[pd.DataFrame]:
        """Load and cache the governance CSV (remuneracao_total_orgao)."""
        key = "governance"
        if key not in self._dataframes:
            try:
                path = _ensure_fre_csv("fre_cia_aberta_remuneracao_total_orgao_2025.csv")
                # Load with proper encoding and separator (ISO-8859-1 for Brazilian CSV files)
                df = pd.read_csv(path, sep=";", encoding="iso-8859-1")
                self._dataframes[key] = df
                print(f"[CACHE] Loaded governance CSV: {len(df)} rows")
            except Exception as e:
                print(f"[ERROR] Failed to load governance CSV: {e}")
                self._dataframes[key] = None
        return self._dataframes[key]


def get_filtered_csv_data(target_cnpj: str, file_type: str) -> tuple[Optional[str], int]:
    """
    Filter CSV data by CNPJ using singleton-cached DataFrames.
    Returns filtered rows as a Markdown table string for Neo4j Knowledge Graph extraction.
    
    Args:
        target_cnpj: Target CNPJ (with or without formatting)
        file_type: Either "shareholding" or "governance"
        
    Returns:
        Tuple of (markdown_table_string or None, row_count)
    """
    if not target_cnpj or not target_cnpj.strip():
        return None, 0
    
    # Normalize the target CNPJ
    normalized_target = clean_cnpj(target_cnpj)
    
    cache = _CSVCache()
    
    if file_type.lower() == "shareholding":
        df = cache.get_shareholding()
        if df is None or df.empty:
            return None, 0
        
        # Normalize CNPJ_Companhia column and filter
        df_normalized = df.copy()
        df_normalized["CNPJ_Normalized"] = df_normalized["CNPJ_Companhia"].apply(clean_cnpj)
        filtered = df_normalized[df_normalized["CNPJ_Normalized"] == normalized_target]
        
        if filtered.empty:
            return None, 0
        
        # Select Neo4j-critical columns for Knowledge Graph construction
        # Identity Layer: CNPJ_Companhia, Nome_Companhia, CPF_CNPJ_Acionista, Acionista
        # Relationship Layer: Percentual_Total_Acoes_Circulacao, Acionista_Controlador, Participante_Acordo_Acionistas
        required_cols = [
            "CNPJ_Companhia",
            "Nome_Companhia", 
            "Acionista",
            "CPF_CNPJ_Acionista",
            "Percentual_Total_Acoes_Circulacao",
            "Acionista_Controlador",
            "Participante_Acordo_Acionistas"
        ]
        
        # Filter to available columns (some may not exist)
        available_cols = [col for col in required_cols if col in filtered.columns]
        filtered_selected = filtered[available_cols]

        # Normalize ID columns to digit-only strings (preserve leading zeros)
        for col in ("CNPJ_Companhia", "CPF_CNPJ_Acionista"):
            if col in filtered_selected.columns:
                filtered_selected[col] = (
                    filtered_selected[col].astype(str).str.replace(r"\D", "", regex=True)
                )

        # Convert to markdown table
        markdown_table = filtered_selected.to_markdown(index=False)
        return markdown_table, len(filtered_selected)
    
    elif file_type.lower() == "governance":
        df = cache.get_governance()
        if df is None or df.empty:
            return None, 0
        
        # Normalize CNPJ_Companhia column and filter
        df_normalized = df.copy()
        df_normalized["CNPJ_Normalized"] = df_normalized["CNPJ_Companhia"].apply(clean_cnpj)
        filtered = df_normalized[df_normalized["CNPJ_Normalized"] == normalized_target]
        
        if filtered.empty:
            return None, 0
        
        # Select only Orgao_Administracao column for governance
        required_cols = ["Orgao_Administracao"]
        available_cols = [col for col in required_cols if col in filtered.columns]
        
        if available_cols:
            filtered_selected = filtered[available_cols].drop_duplicates()
        else:
            return None, 0
        
        # Convert to markdown table
        markdown_table = filtered_selected.to_markdown(index=False)
        return markdown_table, len(filtered_selected)
    
    else:
        print(f"[WARNING] Unknown file_type: {file_type}. Expected 'shareholding' or 'governance'")
        return None, 0


def _normalize_percentage_value(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "")
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def get_shareholding_owns_relationships(target_cnpj: str) -> tuple[List[Dict], Optional[str], int]:
    """
    Deterministically build :OWNS relationships for a target company from CVM shareholding CSV.

    Returns:
        (relationships, corporate_group_notes, row_count)
    """
    if not target_cnpj or not str(target_cnpj).strip():
        return [], None, 0

    normalized_target = clean_cnpj(target_cnpj)
    if len(normalized_target) != 14:
        return [], None, 0

    cache = _CSVCache()
    df = cache.get_shareholding()
    if df is None or df.empty:
        return [], None, 0

    if "CNPJ_Companhia" not in df.columns:
        return [], None, 0

    df_normalized = df.copy()
    df_normalized["CNPJ_Normalized"] = df_normalized["CNPJ_Companhia"].apply(clean_cnpj)
    filtered = df_normalized[df_normalized["CNPJ_Normalized"] == normalized_target]
    if filtered.empty:
        return [], None, 0

    rel_by_source: Dict[str, Dict] = {}
    for _, row in filtered.iterrows():
        shareholder_id = clean_cnpj(row.get("CPF_CNPJ_Acionista"))
        if len(shareholder_id) == 11:
            source_label = "Person"
        elif len(shareholder_id) == 14:
            source_label = "Company"
        else:
            continue

        shareholder_name = str(row.get("Acionista") or "").strip() or None
        percentage_raw = row.get("Percentual_Total_Acoes_Circulacao")
        is_controller_raw = str(row.get("Acionista_Controlador") or "").strip().upper()
        is_controller = True if is_controller_raw == "S" else False

        existing = rel_by_source.get(shareholder_id)
        if existing is None:
            rel_by_source[shareholder_id] = {
                "source_id": shareholder_id,
                "source_name": shareholder_name,
                "source_label": source_label,
                "target_id": normalized_target,
                "relationship_type": "OWNS",
                "properties": {
                    "percentage": percentage_raw,
                    "is_controller": is_controller,
                },
            }
            continue

        # Deduplicate by keeping the strongest signal for controller and max percentage.
        if existing.get("source_name") is None and shareholder_name:
            existing["source_name"] = shareholder_name
        if existing.get("source_label") != source_label:
            existing["source_label"] = source_label

        existing_props = existing.get("properties") or {}
        existing_props["is_controller"] = bool(existing_props.get("is_controller")) or is_controller

        existing_pct = _normalize_percentage_value(existing_props.get("percentage"))
        new_pct = _normalize_percentage_value(percentage_raw)
        if new_pct is not None and (existing_pct is None or new_pct > existing_pct):
            existing_props["percentage"] = percentage_raw
        existing["properties"] = existing_props

    relationships = list(rel_by_source.values())

    controllers = []
    for rel in relationships:
        if rel.get("source_label") != "Company":
            continue
        props = rel.get("properties") or {}
        pct = _normalize_percentage_value(props.get("percentage"))
        if props.get("is_controller") is True or (pct is not None and pct > 50.0):
            controllers.append(rel)

    corporate_group_notes = None
    if controllers:
        first = controllers[0]
        name = first.get("source_name") or "Unknown"
        corporate_group_notes = f"Controlled by {name}"
        if len(controllers) > 1:
            corporate_group_notes = f"{corporate_group_notes} (+{len(controllers)-1} other controller(s))"
    elif relationships:
        corporate_group_notes = "Independent company"

    return relationships, corporate_group_notes, len(filtered)


def get_search_query(nome_empresa: str, sede: str, search_type: str) -> str:
    """
    Format the search query for a target company enrichment task.
    """

    normalized_type = search_type.lower().strip()

    if normalized_type == "linkedin":
        query = f"{nome_empresa} {sede} LinkedIn perfil oficial"
    elif normalized_type == "cnpj":
        query = f"{nome_empresa} {sede} CNPJ Receita Federal"
    elif normalized_type == "site": 
        query = f"{nome_empresa} {sede} site oficial"
    elif normalized_type == "address": 
        query = f"{nome_empresa} {sede} endereço físico sede"
    else:
        # Fallback: raise error or return empty string to prevent UnboundLocalError
        raise ValueError(f"Unknown search_type: '{search_type}'. Must be one of: linkedin, cnpj, site, official, address")

    return query.strip()


def search_company_web_presence(query: str, max_results: int = 4) -> List[Dict[str, str]]:
    """
    Execute DuckDuckGo search for company presence.
    """

    # Use delay to reduce rate pressure
    time.sleep(random.uniform(0.5, 1.5))

    try:
        cleaned_results: List[Dict[str, str]] = []
        with DDGS() as ddgs:
            for entry in ddgs.text(query, max_results=max_results):
                # Normalize field to keep schema consistent
                title = str(entry.get("title", "")).strip()
                link = str(entry.get("href") or entry.get("link", "")).strip()
                snippet = str(entry.get("body") or entry.get("snippet", "")).strip()

                cleaned_results.append(
                    {
                        "title": title,
                        "link": link,
                        "snippet": snippet,
                    }
                )
        return cleaned_results
    except Exception:
        # Catch exception to keep pipeline alive
        return []


_CNPJ_FORMATTED_RE = re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b")
_CNPJ_DIGITS_RE = re.compile(r"\b\d{14}\b")


def has_cnpj_in_results(results: List[Dict[str, str]]) -> bool:
    """
    Check if search result text includes a plausible CNPJ.
    """
    if not results:
        return False
    for item in results:
        text = " ".join(
            str(item.get(key, "")).strip() for key in ("title", "snippet", "link") if item.get(key)
        )
        if not text:
            continue
        if _CNPJ_FORMATTED_RE.search(text) or _CNPJ_DIGITS_RE.search(text):
            return True
    return False


def build_cnpj_retry_queries(nome_empresa: str, sede: str) -> List[str]:
    """
    Build retry query list for CNPJ discovery.
    """
    base = (nome_empresa or "").strip()
    city = (sede or "").strip()
    if not base:
        return []
    queries = []
    queries.append(f"{base} CNPJ")
    if city:
        queries.append(f"{base} {city} CNPJ")
    queries.append(f"\"{base}\" CNPJ Receita Federal")
    queries.append(f"{base} CNPJ site:gov.br")
    return queries


def merge_search_results(
    primary: List[Dict[str, str]],
    extra: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Merge search results with lightweight deduplication.
    """
    merged: List[Dict[str, str]] = []
    seen = set()
    for item in (primary or []) + (extra or []):
        title = str(item.get("title", "")).strip()
        link = str(item.get("link", "")).strip()
        snippet = str(item.get("snippet", "")).strip()
        if link:
            key = link.lower()
        else:
            key = f"{title}|{snippet}".lower()
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(
            {
                "title": title,
                "link": link,
                "snippet": snippet,
            }
        )
    return merged


def _slugify_company_name(value: str) -> str:
    """
    Build a safe slug for file names.
    """
    text = re.sub(r"[^\w\s-]", "", (value or "").strip().lower())
    text = re.sub(r"[\s_-]+", "-", text).strip("-")
    return text or "company"


def ensure_company_log_context(company: Dict) -> tuple[str, str]:
    """
    Ensure run_id and log_file exist for the company.
    """
    run_id = company.get("run_id")
    if not run_id:
        base_name = company.get("nome_empresa") or company.get("razao_social") or "company"
        slug = _slugify_company_name(str(base_name))
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        suffix = random.randint(1000, 9999)
        run_id = f"{timestamp}-{slug}-{suffix}"
        company["run_id"] = run_id
    log_file = company.get("log_file")
    if not log_file:
        os.makedirs(_LOG_DIR, exist_ok=True)
        log_file = os.path.join(_LOG_DIR, f"{run_id}.log")
        company["log_file"] = log_file
    return run_id, log_file


def log_company_event(
    company: Dict,
    node: str,
    message: str,
    execution_logs: Optional[List[str]] = None,
    also_print: bool = True,
) -> str:
    """
    Emit a prefixed log line for a company and write to its log file.
    """
    if not isinstance(company, dict):
        prefix = f"[unknown|{node}]"
        line = f"{prefix} {message}"
        if also_print:
            print(line)
        if execution_logs is not None:
            execution_logs.append(line)
        return line

    run_id, log_file = ensure_company_log_context(company)
    prefix = f"[{run_id}|{node}]"
    line = f"{prefix} {message}"
    if also_print:
        print(line)
    if execution_logs is not None:
        execution_logs.append(line)
    with _LOG_LOCK:
        with open(log_file, "a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")
    return line


async def fetch_corporate_structure_legacy(cnpj: str) -> Optional[str]:
    """
    [LEGACY - PRESERVED FOR REFERENCE]
    Fetch corporate structure (QSA/Sócio/Acionistas) from cnpj.biz.
    
    This function is deprecated in favor of get_mock_corporate_data() to support
    future CSV-based integration. Kept intact for historical reference and potential
    future resurrection if needed.
    
    Args:
        cnpj: Brazilian CNPJ number (with or without formatting)
        
    Returns:
        Extracted text content of societary structure tables, or None if fetch fails.
        Returns text content only to minimize token usage.
    """
    # Clean CNPJ: remove common formatting characters
    clean_cnpj = cnpj.replace(".", "").replace("/", "").replace("-", "").strip()
    
    if not clean_cnpj or len(clean_cnpj) != 14:
        return None
    
    url = f"https://cnpj.biz/{clean_cnpj}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # Add timeout and retry logic for robustness
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse HTML and find tables containing target keywords
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Keywords to search for in table headers or container text
        target_keywords = ["Sócio", "Acionistas", "QSA", "Sócio-Gerente", "diretor", "presidente"]
        
        extracted_texts = []
        
        # Search for tables
        for table in soup.find_all("table"):
            table_text = table.get_text(strip=True)
            # Check if any target keyword is in the table
            if any(keyword in table_text for keyword in target_keywords):
                extracted_texts.append(table_text)
        
        # Search for divs with specific classes or data attributes
        for div in soup.find_all("div"):
            div_text = div.get_text(strip=True)
            if any(keyword in div_text for keyword in target_keywords):
                # Limit to avoid overly large extractions
                if len(div_text) < 5000:
                    extracted_texts.append(div_text)
        
        # Return consolidated text
        if extracted_texts:
            # Deduplicate and join, limiting total length for token efficiency
            unique_texts = list(dict.fromkeys(extracted_texts))
            consolidated = "\n\n".join(unique_texts)
            return consolidated[:3000] if len(consolidated) > 3000 else consolidated
        
        return None
        
    except requests.Timeout:
        return None
    except requests.ConnectionError:
        return None
    except Exception:
        # Gracefully handle any parsing or request errors
        return None
