"""Persist and summarize LLM token usage and cost across runs."""

import json
import os
import threading
from datetime import datetime, timezone
from typing import Dict

_LOCK = threading.Lock()
_DATA_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "../data"))
_LLM_USAGE_PATH = os.path.join(_DATA_DIR, "llm_usage_totals.json")


def _default_totals() -> Dict[str, float]:
    return {
        "runs": 0,
        "total_requests": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_tokens": 0,
        "total_cost_usd": 0.0,
        "updated_at": None,
    }


def load_cumulative_llm_usage() -> Dict[str, float]:
    """
    Load cumulative LLM usage totals from disk.
    """
    if not os.path.exists(_LLM_USAGE_PATH):
        return _default_totals()
    try:
        with open(_LLM_USAGE_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            return _default_totals()
        base = _default_totals()
        base.update({k: data.get(k, base[k]) for k in base.keys()})
        return base
    except (OSError, json.JSONDecodeError):
        return _default_totals()


def update_cumulative_llm_usage(run_usage: Dict[str, float]) -> Dict[str, float]:
    """
    Add a run usage snapshot into the cumulative totals and persist to disk.
    """
    os.makedirs(_DATA_DIR, exist_ok=True)
    with _LOCK:
        totals = load_cumulative_llm_usage()
        totals["runs"] = int(totals.get("runs", 0)) + 1
        totals["total_requests"] = int(totals.get("total_requests", 0)) + int(run_usage.get("requests", 0))
        totals["total_input_tokens"] = int(totals.get("total_input_tokens", 0)) + int(run_usage.get("input_tokens", 0))
        totals["total_output_tokens"] = int(totals.get("total_output_tokens", 0)) + int(run_usage.get("output_tokens", 0))
        totals["total_tokens"] = int(totals.get("total_tokens", 0)) + int(run_usage.get("total_tokens", 0))
        totals["total_cost_usd"] = float(totals.get("total_cost_usd", 0.0)) + float(run_usage.get("cost_usd", 0.0))
        totals["updated_at"] = datetime.now(timezone.utc).isoformat()
        with open(_LLM_USAGE_PATH, "w", encoding="utf-8") as handle:
            json.dump(totals, handle, ensure_ascii=True, indent=2)
        return totals
