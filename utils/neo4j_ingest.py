import os
import threading
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase

_driver = None
_driver_lock = threading.Lock()
_constraints_ready = False


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_neo4j_driver():
    global _driver
    if _driver is not None:
        return _driver
    with _driver_lock:
        if _driver is None:
            uri = _get_env("NEO4J_URI")
            user = _get_env("NEO4J_USER")
            password = _get_env("NEO4J_PASSWORD")
            _driver = GraphDatabase.driver(uri, auth=(user, password))
    return _driver


def _ensure_constraints(driver) -> None:
    global _constraints_ready
    if _constraints_ready:
        return
    with _driver_lock:
        if _constraints_ready:
            return
        database = os.getenv("NEO4J_DATABASE") or None
        constraint_queries = [
            "CREATE CONSTRAINT company_cnpj IF NOT EXISTS FOR (c:Company) REQUIRE c.cnpj IS UNIQUE",
            "CREATE CONSTRAINT person_cpf IF NOT EXISTS FOR (p:Person) REQUIRE p.cpf IS UNIQUE",
            "CREATE CONSTRAINT brand_name IF NOT EXISTS FOR (b:Brand) REQUIRE b.name IS UNIQUE",
        ]
        with driver.session(database=database) as session:
            for query in constraint_queries:
                session.run(query)
        _constraints_ready = True


def _normalize_percentage(value: Any) -> Optional[float]:
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


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "s"}:
        return True
    if text in {"false", "f", "no", "n"}:
        return False
    return None


def _valid_cnpj(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) != 14 or not text.isdigit():
        return None
    return text


def _build_payload(companies: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    company_rows: List[Dict[str, Any]] = []
    relationship_rows: List[Dict[str, Any]] = []

    for company in companies:
        cnpj = _valid_cnpj(company.get("primary_cnpj"))
        if not cnpj:
            continue
        company_rows.append(
            {
                "cnpj": cnpj,
                "name": company.get("nome_empresa"),
                "sede": company.get("sede"),
                "setor": company.get("setor"),
                "receita_liquida_milhoes": company.get("receita_liquida_milhoes"),
                "lucro_liquido_milhoes": company.get("lucro_liquido_milhoes"),
                "official_website": company.get("official_website"),
                "linkedin_url": company.get("linkedin_url"),
                "about_page_url": company.get("about_page_url"),
                "institutional_description": company.get("institutional_description"),
                "institutional_summary": company.get("institutional_summary"),
                "corporate_group_notes": company.get("corporate_group_notes"),
                "brands": company.get("found_brands") or [],
            }
        )

        relationships = company.get("relationships") or []
        if not isinstance(relationships, list):
            continue
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            source_id = str(rel.get("source_id", "")).strip()
            target_id = str(rel.get("target_id", "")).strip()
            if not source_id or not target_id:
                continue
            props = rel.get("properties") or {}
            relationship_rows.append(
                {
                    "source_id": source_id,
                    "source_name": rel.get("source_name"),
                    "source_label": rel.get("source_label") or "Company",
                    "target_id": target_id,
                    "relationship_type": rel.get("relationship_type") or "OWNS",
                    "percentage": _normalize_percentage(props.get("percentage")),
                    "is_controller": _normalize_bool(props.get("is_controller")),
                }
            )

    return company_rows, relationship_rows


def ingest_companies_batch(companies: List[Dict[str, Any]]) -> List[str]:
    if not companies:
        return []
    driver = get_neo4j_driver()
    _ensure_constraints(driver)
    database = os.getenv("NEO4J_DATABASE") or None
    company_rows, relationship_rows = _build_payload(companies)
    ingested_ids = [row["cnpj"] for row in company_rows]
    if not company_rows:
        return []

    company_query = (
        "UNWIND $companies AS row "
        "MERGE (c:Company {cnpj: row.cnpj}) "
        "SET c.name = row.name, "
        "c.sede = row.sede, "
        "c.setor = row.setor, "
        "c.receita_liquida_milhoes = row.receita_liquida_milhoes, "
        "c.lucro_liquido_milhoes = row.lucro_liquido_milhoes, "
        "c.official_website = row.official_website, "
        "c.linkedin_url = row.linkedin_url, "
        "c.about_page_url = row.about_page_url, "
        "c.institutional_description = row.institutional_description, "
        "c.institutional_summary = row.institutional_summary, "
        "c.corporate_group_notes = row.corporate_group_notes "
        "WITH c, row "
        "UNWIND row.brands AS brand_name "
        "MERGE (b:Brand {name: brand_name}) "
        "MERGE (c)-[:HAS_BRAND]->(b)"
    )

    relationship_query = (
        "UNWIND $relationships AS rel "
        "WITH rel "
        "WHERE rel.source_id IS NOT NULL AND rel.target_id IS NOT NULL "
        "MERGE (target:Company {cnpj: rel.target_id}) "
        "FOREACH (_ IN CASE WHEN rel.source_label = 'Person' THEN [1] ELSE [] END | "
        "  MERGE (source:Person {cpf: rel.source_id}) "
        "  SET source.name = rel.source_name "
        "  FOREACH (__ IN CASE WHEN rel.relationship_type = 'SUBSIDIARY_OF' THEN [1] ELSE [] END | "
        "    MERGE (source)-[r:SUBSIDIARY_OF]->(target) "
        "    SET r.percentage = rel.percentage "
        "  ) "
        "  FOREACH (__ IN CASE WHEN rel.relationship_type = 'OWNS' THEN [1] ELSE [] END | "
        "    MERGE (source)-[r:OWNS]->(target) "
        "    SET r.percentage = rel.percentage, r.is_controller = rel.is_controller "
        "  ) "
        ") "
        "FOREACH (_ IN CASE WHEN rel.source_label <> 'Person' THEN [1] ELSE [] END | "
        "  MERGE (source:Company {cnpj: rel.source_id}) "
        "  SET source.name = rel.source_name "
        "  FOREACH (__ IN CASE WHEN rel.relationship_type = 'SUBSIDIARY_OF' THEN [1] ELSE [] END | "
        "    MERGE (source)-[r:SUBSIDIARY_OF]->(target) "
        "    SET r.percentage = rel.percentage "
        "  ) "
        "  FOREACH (__ IN CASE WHEN rel.relationship_type = 'OWNS' THEN [1] ELSE [] END | "
        "    MERGE (source)-[r:OWNS]->(target) "
        "    SET r.percentage = rel.percentage, r.is_controller = rel.is_controller "
        "  ) "
        ")"
    )

    with driver.session(database=database) as session:
        session.run(company_query, companies=company_rows)
        if relationship_rows:
            session.run(relationship_query, relationships=relationship_rows)

    return ingested_ids
