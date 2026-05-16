# Databricks notebook source
# DBTITLE 1,Session logger
# MAGIC %md
# MAGIC # Session Logger
# MAGIC
# MAGIC Persists every agent invocation to `realestate.gold.agent_sessions` for the
# MAGIC future dashboard panel and post-hoc analysis.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Imports and config
import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List

TARGET_TABLE = "realestate.gold.agent_sessions"

# COMMAND ----------

# DBTITLE 1,Helpers to derive structured fields
def _extract_cities(plan: Dict[str, Any]) -> List[str]:
    cities = set()
    for call in plan.get("tool_calls", []) or []:
        params = call.get("params") or {}
        for key in ("city", "us_city", "co_city"):
            v = params.get(key)
            if isinstance(v, str) and v.strip():
                cities.add(v.strip())
    return sorted(cities)


def _country_filter(plan: Dict[str, Any]) -> str:
    countries = set()
    for call in plan.get("tool_calls", []) or []:
        params = call.get("params") or {}
        c = params.get("country_code")
        if c in ("US", "CO"):
            countries.add(c)
        if params.get("us_city"):
            countries.add("US")
        if params.get("co_city"):
            countries.add("CO")
    if len(countries) > 1:
        return "BOTH"
    if len(countries) == 1:
        return next(iter(countries))
    return "UNKNOWN"


def _tool_names(evidence: Dict[str, Any]) -> List[str]:
    return list((evidence.get("tools") or {}).keys())


def _record_count(evidence: Dict[str, Any]) -> int:
    total = 0
    for v in (evidence.get("tools") or {}).values():
        if isinstance(v, list):
            total += len(v)
        elif isinstance(v, dict):
            # Treat a non-empty dict as 1 record unless it has a 'message' key.
            if "message" not in v:
                total += 1
    return total


def _structured_results(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Compact JSON with the top-level numeric facts only — strips long strings."""
    out: Dict[str, Any] = {}
    for tool_name, value in (evidence.get("tools") or {}).items():
        if isinstance(value, list):
            out[tool_name] = {"count": len(value)}
            if value and isinstance(value[0], dict):
                for k, v in value[0].items():
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        out[tool_name][f"first_{k}"] = v
        elif isinstance(value, dict):
            out[tool_name] = {}
            for k, v in value.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    out[tool_name][k] = v
                elif isinstance(v, str) and len(v) < 64:
                    out[tool_name][k] = v
    return out

# COMMAND ----------

# DBTITLE 1,log_session
def log_session(
    question: str,
    plan: Dict[str, Any],
    answer: str,
    evidence: Dict[str, Any],
    context: List[Dict[str, Any]] = None,
    start_time: float = None,
) -> str:
    """Insert a single row into gold.agent_sessions and return the session_id."""
    session_id = str(uuid.uuid4())
    now = datetime.utcnow()
    latency = (time.time() - start_time) if start_time else None

    row = {
        "session_id": session_id,
        "timestamp": now,
        "user_question": question,
        "intent": plan.get("intent") or "unknown",
        "planner_reasoning": plan.get("reasoning") or "",
        "plan_json": json.dumps(plan, default=str),
        "answer_text": answer or "",
        "structured_results_json": json.dumps(_structured_results(evidence), default=str),
        "country_filter": _country_filter(plan),
        "cities_mentioned": ",".join(_extract_cities(plan)),
        "tool_names_used": ",".join(_tool_names(evidence)),
        "evidence_record_count": _record_count(evidence),
        "refinement_applied": bool(evidence.get("refinement_log")),
        "context_turns_used": len(context or []),
        "latency_seconds": float(latency) if latency is not None else None,
    }
    spark.createDataFrame([row]).write.mode("append").saveAsTable(TARGET_TABLE)
    return session_id

print("Session logger loaded.")
