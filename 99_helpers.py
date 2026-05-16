# Databricks notebook source
# DBTITLE 1,Real Estate Agent - Shared Helpers
# MAGIC %md
# MAGIC # Real Estate Agent — Shared Helpers
# MAGIC
# MAGIC Functions imported by every ingest, silver, gold, and agent notebook via:
# MAGIC ```
# MAGIC %run ./99_helpers
# MAGIC ```
# MAGIC
# MAGIC **Provides:**
# MAGIC * `get_secret(key)` — read from Databricks Secrets scope `realestate`
# MAGIC * `geocode_address(address, city, country_code)` — Nominatim geocoder, rate-limited
# MAGIC * `usd_to_cop(usd)`, `cop_to_usd(cop)` — currency conversion via bronze rate table
# MAGIC * `clean_limit(value, default, low, high)` — clamp a limit parameter
# MAGIC * `extract_json_object(text)` — extract first JSON object from an LLM response
# MAGIC * `none_if_nullish(value)` — map LLM placeholders (`null`, `n/a`, ...) to `None`
# MAGIC
# MAGIC Functions are defined at notebook top-level so `%run` makes them available
# MAGIC in the caller. This notebook has **no side effects** when imported other than
# MAGIC selecting the `realestate` catalog so downstream notebooks resolve tables
# MAGIC consistently.

# COMMAND ----------

# DBTITLE 1,Imports
import json
import re
import time
from typing import Any

import requests

# COMMAND ----------

# DBTITLE 1,Catalog
# Use the realestate catalog so unqualified table references in downstream
# notebooks resolve to `realestate.<schema>.<table>`.
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Constants
# Databricks Secrets scope shared by every notebook in this agent.
SECRETS_SCOPE = "realestate"

# Fully qualified bronze table holding daily FX rates. Rows are expected to be
# stored as COP per 1 USD with columns: from_currency, to_currency, rate,
# rate_date (or ingest_time). Most recent row by rate_date wins.
EXCHANGE_RATES_TABLE = "realestate.bronze.exchange_rates"

# Nominatim public endpoint. Free use is limited to 1 request/second and
# requires a distinct, contactable User-Agent.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "realestate-agent-databricks/1.0"
NOMINATIM_MIN_INTERVAL_SECONDS = 1.1
NOMINATIM_REQUEST_TIMEOUT_SECONDS = 30

# Set of strings (case-insensitive, trimmed) that should be treated as missing
# when an LLM returns them in place of a real value.
_NULLISH_STRINGS = {
    "null",
    "none",
    "n/a",
    "na",
    "unknown",
    "not specified",
}

# COMMAND ----------

# DBTITLE 1,Databricks Secrets
def get_secret(key: str) -> str:
    """
    Read a secret from the Databricks Secrets `realestate` scope.

    Args:
        key: Secret key (e.g. 'openai_api_key', 'rentcast_api_key').

    Returns:
        The secret value as a string.

    Raises:
        Exception: Re-raises any error from `dbutils.secrets.get` (typically
            because the scope or key is missing). Callers should ensure the
            secret exists before running pipelines that depend on it.
    """
    return dbutils.secrets.get(scope=SECRETS_SCOPE, key=key)  # noqa: F821 - dbutils provided by Databricks runtime

# COMMAND ----------

# DBTITLE 1,Geocoding
def geocode_address(
    address: str,
    city: str,
    country_code: str,
) -> tuple[float, float]:
    """
    Geocode a street address using the free Nominatim API.

    Nominatim's free tier requires:
      * A descriptive, contactable `User-Agent` header (set globally for this
        agent to `realestate-agent-databricks/1.0`).
      * No more than 1 request per second — we sleep ~1.1s before every call.

    Args:
        address: Street address line (e.g. '123 Main St').
        city: City name (e.g. 'Austin' or 'Medellin').
        country_code: ISO 3166-1 alpha-2 country code (e.g. 'us', 'co').
            Used to bias the search to that country.

    Returns:
        A `(lat, lon)` tuple of floats. Both values are `None` when Nominatim
        returns no result, when the call errors, or when inputs are blank.
    """
    if not address or not city:
        return (None, None)

    # Always honour the public-instance rate limit before issuing the request.
    time.sleep(NOMINATIM_MIN_INTERVAL_SECONDS)

    query = f"{address}, {city}"
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
    }
    if country_code:
        params["countrycodes"] = country_code.lower()

    headers = {"User-Agent": NOMINATIM_USER_AGENT}

    try:
        response = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=headers,
            timeout=NOMINATIM_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            return (None, None)
        payload = response.json()
    except Exception:
        return (None, None)

    if not payload:
        return (None, None)

    first = payload[0]
    try:
        lat = float(first["lat"])
        lon = float(first["lon"])
    except (KeyError, TypeError, ValueError):
        return (None, None)

    return (lat, lon)

# COMMAND ----------

# DBTITLE 1,Currency conversion
def _latest_cop_per_usd_rate() -> float:
    """
    Return the most recent COP-per-USD rate from the bronze exchange table.

    The bronze table is expected to store rows like:
        from_currency='COP', to_currency='USD', rate=<COP per 1 USD>
    Most-recent is determined by `rate_date` when present, otherwise by
    `ingest_time` as a fallback.

    Raises:
        ValueError: If no matching row is found.
    """
    df = spark.sql(
        f"""
        SELECT rate
        FROM {EXCHANGE_RATES_TABLE}
        WHERE from_currency = 'COP' AND to_currency = 'USD'
        ORDER BY COALESCE(rate_date, ingest_time) DESC
        LIMIT 1
        """
    )
    row = df.first()
    if row is None or row["rate"] is None:
        raise ValueError(
            f"No COP->USD exchange rate found in {EXCHANGE_RATES_TABLE}. "
            "Ingest a rate before calling usd_to_cop / cop_to_usd."
        )
    return float(row["rate"])


def usd_to_cop(usd: float) -> float:
    """
    Convert a USD amount to COP using the latest bronze exchange rate.

    The bronze table stores COP per 1 USD, so: `cop = usd * rate`.
    """
    if usd is None:
        return None
    rate = _latest_cop_per_usd_rate()
    return float(usd) * rate


def cop_to_usd(cop: float) -> float:
    """
    Convert a COP amount to USD using the latest bronze exchange rate.

    The bronze table stores COP per 1 USD, so: `usd = cop / rate`.
    """
    if cop is None:
        return None
    rate = _latest_cop_per_usd_rate()
    if rate == 0:
        raise ValueError("Exchange rate is zero; cannot divide COP by zero.")
    return float(cop) / rate

# COMMAND ----------

# DBTITLE 1,Limit clamping
def clean_limit(value, default: int = 10, low: int = 1, high: int = 50) -> int:
    """
    Clamp a `limit` parameter to a safe integer range.

    Args:
        value: Caller-supplied limit. May be `None`, a string, a float, etc.
        default: Value to use when `value` cannot be parsed as an int.
        low: Minimum allowed value (inclusive).
        high: Maximum allowed value (inclusive).

    Returns:
        An integer in `[low, high]`.
    """
    try:
        if value is None:
            parsed = default
        else:
            parsed = int(value)
    except (TypeError, ValueError):
        parsed = default

    if parsed < low:
        return low
    if parsed > high:
        return high
    return parsed

# COMMAND ----------

# DBTITLE 1,LLM response helpers
def extract_json_object(text: str) -> dict:
    """
    Extract the first JSON object from a model response.

    Handles common LLM quirks:
      * Leading/trailing whitespace.
      * Fenced code blocks (```json ... ``` or ``` ... ```).
      * Prose surrounding the JSON object — falls back to slicing from the
        first `{` to the last `}`.

    Args:
        text: Raw model response.

    Returns:
        A `dict` parsed from the first JSON object in `text`.

    Raises:
        ValueError: If `text` is empty or no JSON object can be parsed.
    """
    if not text:
        raise ValueError("Empty model response")

    cleaned = text.strip()
    # Remove fenced code block markers if the model returns them.
    cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Fallback: find the first {...} span.
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        parsed = json.loads(cleaned[start:end + 1])
        if isinstance(parsed, dict):
            return parsed

    raise ValueError(f"Could not parse JSON object from model response: {text[:500]}")


def none_if_nullish(value: Any) -> Any:
    """
    Convert common LLM placeholder strings to real `None`.

    The set of strings treated as missing (case-insensitive, trimmed) is:
    `null`, `none`, `n/a`, `na`, `unknown`, `not specified`, plus the empty
    string. Non-string values are returned unchanged.
    """
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return None
        if cleaned.lower() in _NULLISH_STRINGS:
            return None
        return cleaned
    return value

# COMMAND ----------

# DBTITLE 1,Sanity check
print("Real estate helpers loaded. Secrets scope =", SECRETS_SCOPE)
