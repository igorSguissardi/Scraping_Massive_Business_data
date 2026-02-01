"""Utility helpers for search-driven enrichment."""

import asyncio
import random
import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS


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


async def fetch_corporate_structure(cnpj: str) -> Optional[str]:
    """
    Fetch corporate structure (QSA/Sócio/Acionistas) from cnpj.biz.
    
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
