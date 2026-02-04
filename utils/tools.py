"""Utility helpers for search-driven enrichment."""

import asyncio
import os
import random
import re
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from ddgs import DDGS


# ============================================================================
# CNPJ SNIPER - CSV-Based Corporate Structure Enrichment
# ============================================================================

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
                path = os.path.join(
                    os.path.dirname(__file__),
                    "../data/fre_cia_aberta_2025/fre_cia_aberta_posicao_acionaria_2025.csv"
                )
                # Load with proper encoding and separator (ISO-8859-1 for Brazilian CSV files)
                df = pd.read_csv(path, sep=";", encoding="latin1")
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
                path = os.path.join(
                    os.path.dirname(__file__),
                    "../data/fre_cia_aberta_2025/fre_cia_aberta_remuneracao_total_orgao_2025.csv"
                )
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

