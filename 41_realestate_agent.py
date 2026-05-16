# Databricks notebook source
# DBTITLE 1,Real Estate Agent — LLM Orchestrator
# MAGIC %md
# MAGIC # Real Estate Agent — LLM Orchestrator
# MAGIC
# MAGIC LLM-driven tool selection. The planner reads the user's question and
# MAGIC produces an explicit list of tool calls with parameters. The executor
# MAGIC validates and runs them against Delta tables. A refinement pass adjusts
# MAGIC parameters when tools return sparse data. The synthesizer writes a
# MAGIC narrative answer using only the returned evidence.
# MAGIC
# MAGIC Multi-turn `chat()` maintains conversation context across calls.

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# MAGIC %run ./40_agent_tools

# COMMAND ----------

# MAGIC %run ./42_session_logger

# COMMAND ----------

# DBTITLE 1,Imports and config
import json
import time
from typing import Any, Dict, List, Optional

try:
    from mlflow.deployments import get_deploy_client
except Exception:
    get_deploy_client = None

PIPELINE_NAME = "41_realestate_agent"

MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_LIMIT = 10
MAX_EVIDENCE_CHARS = 28000
MAX_CONTEXT_TURNS = 3
MAX_REFINEMENT_PASSES = 1

SUPPORTED_COUNTRIES = ["US", "CO"]
SUPPORTED_CO_CITIES = ["Bogota", "Medellin"]

ALLOWED_TOOLS = set(TOOL_REGISTRY.keys())

ALLOWED_PROPERTY_TYPES = {"single_family", "apartment", "condo", "townhouse"}
ALLOWED_AMENITY_TYPES  = {"grocery", "hospital", "park", "transit_stop", "pharmacy", "school", "restaurant"}

PARAM_RULES = {
    "country_code":      lambda v: v in ("US", "CO"),
    "city":              lambda v: isinstance(v, str) and len(v) > 0,
    "us_city":           lambda v: isinstance(v, str) and len(v) > 0,
    "co_city":           lambda v: v in ("Bogota", "Medellin"),
    "zip_or_municipio":  lambda v: isinstance(v, str) and 0 < len(v) <= 64,
    "barrio":            lambda v: isinstance(v, str) and 0 < len(v) <= 64,
    "limit":             lambda v: isinstance(v, int) and 1 <= v <= 50,
    "months_back":       lambda v: isinstance(v, int) and 1 <= v <= 24,
    "years_back":        lambda v: isinstance(v, int) and 1 <= v <= 10,
    "min_price_usd":     lambda v: isinstance(v, (int, float)) and v >= 0,
    "max_price_usd":     lambda v: isinstance(v, (int, float)) and v > 0,
    "bedrooms_min":      lambda v: isinstance(v, int) and 0 <= v <= 10,
    "bathrooms_min":     lambda v: isinstance(v, (int, float)) and 0 <= v <= 10,
    "property_type":     lambda v: v in ALLOWED_PROPERTY_TYPES,
    "amenity_types":     lambda v: isinstance(v, list) and all(t in ALLOWED_AMENITY_TYPES for t in v),
    "annual_income_usd": lambda v: isinstance(v, (int, float)) and v > 0,
    "down_payment_usd":  lambda v: isinstance(v, (int, float)) and v >= 0,
}

CLAMP_RULES = {
    "limit":         (1, 50),
    "months_back":   (1, 24),
    "years_back":    (1, 10),
    "bedrooms_min":  (0, 10),
    "bathrooms_min": (0.0, 10.0),
}

print(f"Real estate agent configured with model: {MODEL_ENDPOINT}")
print(f"Tools available: {sorted(ALLOWED_TOOLS)}")

# COMMAND ----------

# DBTITLE 1,Foundation model helpers
def _model_client():
    if get_deploy_client is None:
        raise RuntimeError("mlflow.deployments.get_deploy_client unavailable.")
    return get_deploy_client("databricks")


def _extract_chat_content(response: Any) -> str:
    if response is None:
        return ""
    if isinstance(response, str):
        return response
    if isinstance(response, dict):
        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                msg = first.get("message") or {}
                if isinstance(msg, dict) and msg.get("content") is not None:
                    return str(msg.get("content"))
                if first.get("text") is not None:
                    return str(first.get("text"))
        if response.get("content") is not None:
            return str(response.get("content"))
    return str(response)


def call_foundation_model(messages: List[Dict[str, str]], max_tokens: int = 1500, temperature: float = 0.1) -> str:
    client = _model_client()
    response = client.predict(
        endpoint=MODEL_ENDPOINT,
        inputs={"messages": messages, "max_tokens": max_tokens, "temperature": temperature},
    )
    return _extract_chat_content(response)

# COMMAND ----------

# DBTITLE 1,Colombian city normalization
def _normalize_co_city(name: str) -> Optional[str]:
    if not name:
        return None
    try:
        from unidecode import unidecode
        stripped = unidecode(name).strip().title()
    except Exception:
        stripped = name.strip().title()
    if stripped in ("Bogota", "Bogotá", "Bogotad.C", "Bogotadc"):
        return "Bogota"
    if stripped in ("Medellin", "Medellín"):
        return "Medellin"
    return stripped

# COMMAND ----------

# DBTITLE 1,Heuristic fallback planner
def _detect_city(q: str, context: List[Dict] = None) -> tuple[Optional[str], Optional[str]]:
    ql = q.lower()
    if "bogot" in ql:
        return ("Bogota", "CO")
    if "medell" in ql:
        return ("Medellin", "CO")
    for us in ["austin", "miami", "atlanta", "houston", "chicago", "new york",
              "los angeles", "denver", "phoenix", "tucson", "dallas", "nashville"]:
        if us in ql:
            return (us.title(), "US")
    # Walk context for a previously-mentioned city.
    for turn in reversed(context or []):
        prev = (turn.get("question") or "") + " " + (turn.get("answer") or "")
        prev_lower = prev.lower()
        if "bogot" in prev_lower:
            return ("Bogota", "CO")
        if "medell" in prev_lower:
            return ("Medellin", "CO")
    return (None, None)


def _heuristic_plan(question: str, context: List[Dict] = None) -> Dict[str, Any]:
    q = question.lower()
    city, country_code = _detect_city(q, context)
    tool_calls = []

    if city:
        tool_calls.append({
            "tool": "get_neighborhood_profile",
            "params": {"country_code": country_code, "city": city},
        })
        tool_calls.append({
            "tool": "get_market_trends",
            "params": {"country_code": country_code, "city": city, "months_back": 12},
        })

    if any(w in q for w in ["school", "educat", "kid", "child", "family"]):
        tool_calls.append({"tool": "get_school_rankings",
                           "params": {"country_code": country_code or "CO",
                                      "city": city or "Medellin", "limit": 8}})
    if any(w in q for w in ["safe", "crime", "danger", "secur"]):
        tool_calls.append({"tool": "get_crime_stats",
                           "params": {"country_code": country_code or "CO",
                                      "city": city or "Medellin", "months_back": 12}})
    if any(w in q for w in ["flood", "quake", "earthquake", "wildfire", "risk", "hazard", "disaster", "landslide"]):
        tool_calls.append({"tool": "get_hazard_risks",
                           "params": {"country_code": country_code or "CO",
                                      "city": city or "Medellin"}})
    if any(w in q for w in ["afford", "budget", "income", "mortgage", "down payment"]):
        tool_calls.append({"tool": "get_affordability_analysis",
                           "params": {"country_code": country_code or "CO",
                                      "city": city or "Medellin",
                                      "annual_income_usd": 70000, "down_payment_usd": 20000}})
    if any(w in q for w in ["compare", "vs", "versus", "difference"]):
        tool_calls.append({"tool": "compare_cities",
                           "params": {"us_city": "Miami", "co_city": city or "Medellin"}})

    if not tool_calls:
        tool_calls = [
            {"tool": "search_listings",
             "params": {"country_code": "CO", "city": "Medellin", "limit": 10}},
            {"tool": "get_market_trends",
             "params": {"country_code": "CO", "city": "Medellin", "months_back": 12}},
        ]

    return {
        "intent": "heuristic_fallback",
        "reasoning": "LLM planner unavailable. Using broad heuristic tool selection.",
        "tool_calls": tool_calls[:5],
    }

# COMMAND ----------

# DBTITLE 1,LLM planner
PLANNER_SYSTEM = """
You are a planning assistant for a real estate agent covering the United States and Colombia
(Bogotá and Medellín only). Your job is to decide which tools to call and what parameters
to use, based entirely on what the user is asking.

You must return JSON only. Do not answer the user's question.

Rules:
- You may call between 1 and 5 tools. Only call tools that are genuinely relevant.
- Extract all parameters from the user's natural language. Do not require exact phrasing.
- For Colombian cities, normalize spelling: accept "Medellin", "Medellín", "medellin" → "Medellin".
  Accept "Bogota", "Bogotá", "bogota" → "Bogota".
- If the user implies a price range without stating it exactly ("affordable", "luxury",
  "mid-range"), infer a reasonable USD range from context and note your reasoning.
- If a follow-up question references a prior location ("what about schools there?"),
  extract the location from conversation history.
- The "intent" field is for logging only — it does not constrain which tools you pick.

Available tools:
- search_listings(country_code, city, min_price_usd?, max_price_usd?, bedrooms_min?, bathrooms_min?, property_type?, limit?)
- get_neighborhood_profile(country_code, city, zip_or_municipio?, barrio?)
- get_crime_stats(country_code, city, zip_or_municipio?, months_back?)
- get_school_rankings(country_code, city, zip_or_municipio?, limit?)
- get_weather_summary(country_code, city, years_back?)
- get_hazard_risks(country_code, city, zip_or_municipio?)
- get_market_trends(country_code, city, zip_or_municipio?, months_back?)
- get_area_demographics(country_code, city, zip_or_municipio?)
- get_nearby_amenities(country_code, city, zip_or_municipio?, amenity_types?, limit?)
- compare_cities(us_city, co_city)
- get_affordability_analysis(country_code, city, annual_income_usd, down_payment_usd?)
- get_value_opportunities(country_code, city, max_price_usd?, limit?)
- get_amenity_access(country_code, city, zip_or_municipio?)

Return exactly this JSON shape:
{
  "intent": "short label for logging",
  "reasoning": "1-2 sentences explaining what the user wants and why you chose these tools",
  "tool_calls": [
    {"tool": "exact_function_name", "params": {...}, "why": "one phrase"}
  ]
}
""".strip()


def plan_question(question: str, context: List[Dict] = None, use_llm: bool = True) -> Dict[str, Any]:
    if not use_llm:
        return _heuristic_plan(question, context)

    context_block = ""
    if context:
        context_block = "\nConversation context (most recent turns):\n" + json.dumps(
            context[-MAX_CONTEXT_TURNS:], default=str)

    user_msg = f"User question: {question}{context_block}\n\nReturn JSON only."
    try:
        text = call_foundation_model(
            [{"role": "system", "content": PLANNER_SYSTEM},
             {"role": "user", "content": user_msg}],
            max_tokens=800, temperature=0.0,
        )
        parsed = extract_json_object(text)
        if "tool_calls" not in parsed or not isinstance(parsed["tool_calls"], list):
            raise ValueError("planner returned no tool_calls list")
        parsed.setdefault("intent", "unknown")
        parsed.setdefault("reasoning", "")
        return parsed
    except Exception as e:
        print(f"[planner fallback] {type(e).__name__}: {e}")
        return _heuristic_plan(question, context)

# COMMAND ----------

# DBTITLE 1,Plan validation
def _validate_param(name: str, value: Any) -> tuple[bool, Any]:
    """Returns (ok, possibly-clamped-value)."""
    if value is None:
        return (True, None)
    rule = PARAM_RULES.get(name)
    if rule is None:
        return (True, value)   # unknown param — pass through

    try:
        if rule(value):
            return (True, value)
    except Exception:
        pass

    if name in CLAMP_RULES:
        low, high = CLAMP_RULES[name]
        try:
            v = type(low)(value)
            return (True, max(low, min(v, high)))
        except Exception:
            return (False, None)

    return (False, None)


def validate_plan(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the list of validated tool calls (dropped invalids logged)."""
    raw = plan.get("tool_calls") or []
    valid = []
    for call in raw:
        if not isinstance(call, dict):
            continue
        tool = call.get("tool")
        if tool not in ALLOWED_TOOLS:
            print(f"  dropped — unknown tool: {tool}")
            continue
        params = call.get("params") or {}
        if not isinstance(params, dict):
            continue

        clean = {}
        ok = True
        for k, v in params.items():
            if isinstance(v, str) and k in ("city", "co_city"):
                v = _normalize_co_city(v) if v in ("Bogotá", "Medellín") else v
            ok_p, clean_v = _validate_param(k, v)
            if not ok_p:
                print(f"  dropped — param {k}={v!r} invalid for {tool}")
                ok = False
                break
            clean[k] = clean_v

        if not ok:
            continue

        # Country-specific city support check.
        if clean.get("country_code") == "CO":
            c = clean.get("city")
            if c and c not in SUPPORTED_CO_CITIES:
                print(f"  dropped — unsupported CO city: {c}")
                continue
        if "co_city" in clean and clean["co_city"] not in SUPPORTED_CO_CITIES:
            print(f"  dropped — unsupported co_city: {clean['co_city']}")
            continue

        valid.append({"tool": tool, "params": clean, "why": call.get("why", "")})
        if len(valid) >= 5:
            break

    return valid

# COMMAND ----------

# DBTITLE 1,Plan execution
def execute_plan(validated_calls: List[Dict], question: str, reasoning: str = "") -> Dict[str, Any]:
    evidence = {"question": question, "reasoning": reasoning, "tools": {}, "refinement_log": []}
    for i, call in enumerate(validated_calls):
        fn = TOOL_REGISTRY[call["tool"]]
        try:
            result = fn(**call["params"])
        except Exception as e:
            result = {"error": f"{type(e).__name__}: {e}"}
        key = call["tool"] if call["tool"] not in evidence["tools"] else f"{call['tool']}_{i}"
        evidence["tools"][key] = result
    return evidence

# COMMAND ----------

# DBTITLE 1,Refinement pass (LLM-adjusted params for sparse results)
REFINE_SYSTEM = """
You are a parameter-adjustment assistant. Given tools that returned no data, suggest
adjusted parameters more likely to return data.

Rules:
- You may widen price ranges by up to 30%.
- You may relax bedrooms/bathrooms constraints to 0.
- You may broaden geographic scope from barrio to city level (drop barrio/zip).
- Do NOT add new tools — only adjust parameters for the sparse tools listed.

Return JSON only:
{
  "adjustments": [
    {"tool": "<name>", "params": {adjusted params dict}, "change": "what you changed"}
  ]
}
""".strip()


def _is_sparse(result: Any) -> bool:
    if isinstance(result, list):
        return len(result) == 0
    if isinstance(result, dict):
        return "message" in result or "error" in result
    return False


def refine_if_sparse(evidence: Dict, question: str, validated: List[Dict], use_llm: bool = True) -> Dict:
    if not use_llm:
        return evidence

    tool_to_original = {c["tool"]: c["params"] for c in validated}

    for pass_num in range(MAX_REFINEMENT_PASSES):
        sparse = {name: tool_to_original.get(name.split("_")[0], {})
                  for name, val in (evidence.get("tools") or {}).items()
                  if _is_sparse(val) and name in tool_to_original}
        if not sparse:
            break

        prompt = f"User question:\n{question}\n\nSparse tools and their parameters:\n{json.dumps(sparse, default=str, indent=2)}\n\nReturn JSON only."
        try:
            text = call_foundation_model(
                [{"role": "system", "content": REFINE_SYSTEM},
                 {"role": "user", "content": prompt}],
                max_tokens=500, temperature=0.1,
            )
            parsed = extract_json_object(text)
            adjustments = parsed.get("adjustments") or []
        except Exception as e:
            print(f"[refine fallback] {type(e).__name__}: {e}")
            break

        for adj in adjustments:
            tool = adj.get("tool")
            if tool not in ALLOWED_TOOLS:
                continue
            params = adj.get("params") or {}
            clean = {}
            for k, v in params.items():
                ok, clean_v = _validate_param(k, v)
                if ok:
                    clean[k] = clean_v
            try:
                new_result = TOOL_REGISTRY[tool](**clean)
            except Exception as e:
                new_result = {"error": f"{type(e).__name__}: {e}"}
            evidence["tools"][tool] = new_result
            evidence["refinement_log"].append({
                "tool": tool,
                "original_params": tool_to_original.get(tool),
                "adjusted_params": clean,
                "change": adj.get("change", ""),
                "passes_remaining": MAX_REFINEMENT_PASSES - pass_num - 1,
            })

    return evidence

# COMMAND ----------

# DBTITLE 1,Synthesis
SYNTH_SYSTEM = """
You are a real estate advisory assistant helping users — primarily Americans — purchase homes
in the United States and Colombia (Bogotá and Medellín).

Ground rules:
- Answer using only the evidence provided. Do not invent listings, prices, crime rates,
  school scores, or any other statistics.
- If a tool returned no data, say so plainly. Do not fill gaps with general knowledge or
  estimates presented as facts.
- You may use general knowledge only to provide context around real data.

When comparing US and Colombia:
- Always express Colombian prices in USD first, then note the COP equivalent.
- Use normalized 0–100 scores to compare safety and schools across countries —
  do not compare raw numbers directly.
- Contextualize Colombian metrics in terms Americans recognize.
- Note earthquake risk for Colombian cities — often surprising for American buyers.

Format:
- Lead with a direct bottom-line answer.
- Use bullet points for supporting facts.
- End with a caveats section noting data freshness, any sparse results, and the
  COP/USD exchange rate date used (when relevant).
- Suggest 1–2 natural follow-up questions.
""".strip()


def _truncate_evidence(evidence: Dict, max_chars: int = MAX_EVIDENCE_CHARS) -> str:
    text = json.dumps(evidence, ensure_ascii=False, default=str, indent=2)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... [TRUNCATED EVIDENCE]"


def _deterministic_answer(question: str, evidence: Dict) -> str:
    lines = [f"Question: {question}", "", "Evidence summary:"]
    for tool_name, value in (evidence.get("tools") or {}).items():
        if isinstance(value, list):
            lines.append(f"- {tool_name}: {len(value)} rows")
        elif isinstance(value, dict):
            if "message" in value:
                lines.append(f"- {tool_name}: {value['message']}")
            else:
                lines.append(f"- {tool_name}: {len(value)} fields")
    return "\n".join(lines)


def synthesize_answer(question: str, evidence: Dict, context: List[Dict] = None, use_llm: bool = True) -> str:
    if not use_llm:
        return _deterministic_answer(question, evidence)

    evidence_text = _truncate_evidence(evidence)
    context_text = json.dumps((context or [])[-MAX_CONTEXT_TURNS:], default=str)

    user_prompt = (
        f"Conversation context (most recent turns):\n{context_text}\n\n"
        f"Current question:\n{question}\n\n"
        f"Agent reasoning for this response:\n{evidence.get('reasoning', '')}\n\n"
        f"Tool evidence:\n{evidence_text}\n\n"
        f"Write your answer following the system instructions."
    )

    try:
        return call_foundation_model(
            [{"role": "system", "content": SYNTH_SYSTEM},
             {"role": "user", "content": user_prompt}],
            max_tokens=1600, temperature=0.2,
        )
    except Exception as e:
        print(f"[synthesis fallback] {type(e).__name__}: {e}")
        return _deterministic_answer(question, evidence)

# COMMAND ----------

# DBTITLE 1,Public entrypoints
def ask_realestate(
    question: str,
    context: List[Dict] = None,
    use_llm_planner: bool = True,
    use_llm_answer: bool = True,
    show_evidence: bool = False,
) -> Dict[str, Any]:
    start_time = time.time()
    context = (context or [])[-MAX_CONTEXT_TURNS:]

    plan = plan_question(question, context, use_llm=use_llm_planner)
    validated = validate_plan(plan)
    plan["tool_calls"] = validated  # store the validated calls back into plan for logging

    evidence = execute_plan(validated, question, reasoning=plan.get("reasoning", ""))
    evidence = refine_if_sparse(evidence, question, validated, use_llm=use_llm_planner)

    answer = synthesize_answer(question, evidence, context, use_llm=use_llm_answer)

    try:
        log_session(question, plan, answer, evidence, context, start_time)
    except Exception as e:
        print(f"[session logger failed] {type(e).__name__}: {e}")

    result = {"question": question, "plan": plan, "answer": answer}
    if show_evidence:
        result["evidence"] = evidence
    return result


# Session state for multi-turn interaction.
_session_context: List[Dict] = []


def new_session() -> None:
    global _session_context
    _session_context = []


def print_realestate_answer_from_result(result: Dict) -> None:
    print("PLAN:")
    print(json.dumps(result["plan"], indent=2, ensure_ascii=False, default=str))
    print("\nANSWER:")
    print(result["answer"])


def print_realestate_answer(question: str, **kwargs) -> None:
    result = ask_realestate(question, **kwargs)
    print_realestate_answer_from_result(result)


def chat(question: str, **kwargs) -> None:
    """Multi-turn wrapper. Maintains session context automatically."""
    global _session_context
    result = ask_realestate(question, context=_session_context, **kwargs)
    _session_context.append({"question": question, "answer": result["answer"]})
    print_realestate_answer_from_result(result)


def agent_smoke_test() -> Dict[str, Any]:
    """No-LLM dry run to validate planning + tool execution."""
    q = "Find me 3 bedroom homes under $200k in Medellín"
    result = ask_realestate(q, use_llm_planner=False, use_llm_answer=False, show_evidence=True)
    tool_counts = {}
    for name, value in (result["evidence"].get("tools") or {}).items():
        if isinstance(value, list):
            tool_counts[name] = len(value)
        elif isinstance(value, dict):
            tool_counts[name] = list(value.keys())[:5]
    return {
        "plan": result["plan"],
        "tool_counts": tool_counts,
        "answer_preview": result["answer"][:400],
    }


print("Real estate agent loaded.")
print("Try: agent_smoke_test()")
print("Try: print_realestate_answer('How does Medellín compare to Austin?')")
print("Multi-turn: new_session(); chat('...'); chat('...')")

# COMMAND ----------

# DBTITLE 1,Example calls (uncomment to run)
# agent_smoke_test()
# print_realestate_answer("Find me a 3 bedroom home under $200,000 USD in Medellín")
# print_realestate_answer("How does Medellín compare to Austin for an American buying a home?")
# print_realestate_answer("What are the earthquake and flood risks in Bogotá?")
# print_realestate_answer("What are the best schools near El Poblado in Medellín?")
# print_realestate_answer("On $75,000 USD annual income with $30,000 down, what can I afford in Bogotá?")
# print_realestate_answer("Where are the underpriced homes in Bogotá right now?")
# print_realestate_answer("Something quiet with good transit in Bogotá, nothing too pricey")
# new_session()
# chat("I'm an American thinking about buying a place in Medellín")
# chat("What's the safest neighborhood for families?")
# chat("How do the schools compare to what I'd find in Atlanta?")
# chat("What would a place there run me on a $90k salary?")
