"""Utility helpers for search-driven enrichment."""

import random
import time
from typing import Dict, List

from duckduckgo_search import DDGS


def get_search_query(nome_empresa: str, sede: str, search_type: str) -> str:
    """
    Format the search query for a target company enrichment task.
    """

    normalized_type = search_type.lower().strip()

    if normalized_type == "social":
        query = f"{nome_empresa} {sede} LinkedIn official profile"
    elif normalized_type == "cnpj":
        query = f"{nome_empresa} {sede} CNPJ Receita Federal"
    elif normalized_type == "official":
        query = f"{nome_empresa} {sede} site oficial"
    else:
        query = f"{nome_empresa} {sede} corporate presence"

    return query.strip()


def search_company_web_presence(query: str, max_results: int = 5) -> List[Dict[str, str]]:
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
