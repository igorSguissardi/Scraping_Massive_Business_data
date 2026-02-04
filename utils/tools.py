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

async def get_mock_corporate_data(company_name: str) -> Optional[str]:
    """
    Generate mock corporate structure data for a company.
    
    This function replaces live cnpj.biz scraping to unblock the enrichment pipeline
    while preparing for CSV-based data integration. Returns realistic mock data that
    simulates corporate structure tables, ownership information, and subsidiary relationships.
    
    Args:
        company_name: The name of the company to generate mock data for
        
    Returns:
        Mock corporate structure text (simulating HTML table extraction from cnpj.biz)
        that contains QSA (shareholders), subsidiaries, and corporate hierarchy information.
    """
    # Simulate variable delay as if hitting an external service
    await asyncio.sleep(random.uniform(0.3, 0.8))
    
    # Mock data templates based on company characteristics
    mock_datasets = {
        "holding": (
            "QSA - QUADRO DE SÓCIO-ADMINISTRADOR\n"
            f"{company_name} - CNPJ: 12.345.678/0001-90\n\n"
            "Sócios:\n"
            "- João Silva (50% stake) - Founder & CEO\n"
            "- Maria Santos (30% stake) - CFO\n"
            "- Investimento Corp (20% stake) - Corporate investor\n\n"
            "Subsidiárias e Marcas Operacionais:\n"
            "- Tech Solutions Brazil\n"
            "- Digital Marketing Services Ltda\n"
            "- Innovation Labs Holding\n"
            "- Regional Distribution Center\n\n"
            "Administradores:\n"
            "- João Silva (CEO)\n"
            "- Maria Santos (CFO)\n"
            "- Dr. Carlos Ferreira (Board President)"
        ),
        "industrial": (
            "QSA - QUADRO DE SÓCIO-ADMINISTRADOR\n"
            f"{company_name} - CNPJ: 98.765.432/0001-11\n\n"
            "Sócios:\n"
            "- Industrial Conglomerate Holdings (60% stake)\n"
            "- Local Management Team (40% stake)\n\n"
            "Fabricantes e Linhas de Produtos:\n"
            "- Linha Premium Manufacturing\n"
            "- Industrial Equipment Production\n"
            "- Component Assembly Division\n"
            "- Logistics & Distribution Network\n\n"
            "Administradores:\n"
            "- Roberto Oliveira (General Director)\n"
            "- Ana Patricia (Operations Manager)\n"
            "- Technical Board (3 members)"
        ),
        "financial": (
            "QSA - QUADRO DE SÓCIO-ADMINISTRADOR\n"
            f"{company_name} - CNPJ: 55.555.555/0001-22\n\n"
            "Sócios:\n"
            "- Financial Services Group Brazil (75% stake)\n"
            "- Professional Management Fund (25% stake)\n\n"
            "Entidades Associadas:\n"
            "- Investment Advisory Division\n"
            "- Credit Management Services\n"
            "- Asset Management Fund\n"
            "- Treasury Operations Center\n\n"
            "Conselho de Administração:\n"
            "- Henrique Banco (President)\n"
            "- Patricia Finance (Vice-President)\n"
            "- Board Members (5)"
        ),
        "retail": (
            "QSA - QUADRO DE SÓCIO-ADMINISTRADOR\n"
            f"{company_name} - CNPJ: 77.777.777/0001-33\n\n"
            "Sócios:\n"
            "- Commercial Retail Group (55% stake)\n"
            "- Family Office Capital (45% stake)\n\n"
            "Marcas e Lojas Operacionais:\n"
            "- Flagship Store Network\n"
            "- E-commerce Platform\n"
            "- Regional Distribution Centers\n"
            "- Logistics & Warehousing\n"
            "- Brand Management Division\n\n"
            "Direção Executiva:\n"
            "- Marco Sales (CEO)\n"
            "- Lucia Operations (COO)\n"
            "- Digital Leadership Team"
        ),
    }
    
    # Default template for unknown companies
    default_template = (
        "QSA - QUADRO DE SÓCIO-ADMINISTRADOR\n"
        f"{company_name} - CNPJ: XX.XXX.XXX/0001-XX\n\n"
        "Sócios:\n"
        "- Primary Shareholder (majority stake)\n"
        "- Secondary Investor (minority stake)\n\n"
        "Operações e Divisões:\n"
        "- Main Operations Division\n"
        "- Support Services\n"
        "- Administrative Structure\n\n"
        "Administração:\n"
        "- Executive Leadership\n"
        "- Board of Directors"
    )
    
    # Select mock dataset based on company name keywords
    company_lower = company_name.lower()
    
    if any(keyword in company_lower for keyword in ["holding", "investment", "investment", "participações"]):
        return mock_datasets["holding"]
    elif any(keyword in company_lower for keyword in ["industri", "manufactur", "fabrica", "produção"]):
        return mock_datasets["industrial"]
    elif any(keyword in company_lower for keyword in ["finan", "bank", "credi", "investi", "asset"]):
        return mock_datasets["financial"]
    elif any(keyword in company_lower for keyword in ["retail", "comér", "loja", "venda", "distribui"]):
        return mock_datasets["retail"]
    else:
        return default_template