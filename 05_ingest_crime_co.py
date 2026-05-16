# Databricks notebook source
# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Ingest Crime CO
# MAGIC %md
# MAGIC # 05 — Ingest Crime CO
# MAGIC
# MAGIC Loads Colombian crime statistics for Bogotá (Cundinamarca) and Medellín (Antioquia)
# MAGIC from annual files published by **DANE** and **Policía Nacional (SIEDCO / SIJIN)**.
# MAGIC
# MAGIC Neither agency offers a free public API, so this notebook reads
# MAGIC pre-downloaded files from a Databricks volume that an operator stages by hand.
# MAGIC
# MAGIC **Output:** `realestate.bronze.crime_co` — one row per (city, crime_type, year, month).
# MAGIC The notebook is idempotent per (year, source): existing rows for the year and source
# MAGIC being loaded are deleted first, then fresh rows are inserted.

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Use realestate catalog
# MAGIC %sql
# MAGIC USE CATALOG realestate

# COMMAND ----------

# DBTITLE 1,Operator instructions - how to stage crime files
# MAGIC %md
# MAGIC ## Operator instructions — staging the raw files
# MAGIC
# MAGIC Colombia does not publish a stable, key-free API for crime statistics, so the
# MAGIC annual data files must be downloaded by hand and dropped into a Databricks
# MAGIC volume before this notebook runs.
# MAGIC
# MAGIC ### 1. DANE — National Statistics Office
# MAGIC
# MAGIC 1. Open <https://www.datos.gov.co> and search for **"criminalidad"**.
# MAGIC 2. Look for the most recent dataset published by **DANE** covering reported
# MAGIC    crimes by municipio. Common dataset titles:
# MAGIC    - *Estadísticas de criminalidad*
# MAGIC    - *Encuesta de convivencia y seguridad ciudadana (ECSC)*
# MAGIC 3. Download the annual CSV or Excel export covering Bogotá and Medellín.
# MAGIC 4. Rename the file so it begins with `dane_` and contains the four-digit year,
# MAGIC    e.g. `dane_criminalidad_2024.xlsx` or `dane_criminalidad_2024.csv`.
# MAGIC
# MAGIC ### 2. Policía Nacional — SIEDCO / SIJIN
# MAGIC
# MAGIC 1. Open <https://www.policia.gov.co/sijin> (or the SIEDCO statistics page linked
# MAGIC    from that site).
# MAGIC 2. Download the most recent annual report — usually one Excel file per crime
# MAGIC    category (homicidios, hurto a personas, hurto a residencias, lesiones
# MAGIC    personales, etc.). Each file lists counts by municipio and month.
# MAGIC 3. Rename each file so it begins with `policia_` and contains the year,
# MAGIC    e.g. `policia_homicidios_2024.xlsx`.
# MAGIC
# MAGIC ### 3. Upload to the Databricks volume
# MAGIC
# MAGIC Place every file in:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/realestate/bronze/raw/crime_co/
# MAGIC ```
# MAGIC
# MAGIC The volume already exists (`realestate.bronze.raw`) — this notebook creates
# MAGIC the `crime_co/` subdirectory on first run if missing.
# MAGIC
# MAGIC ### 4. File naming convention
# MAGIC
# MAGIC The parser routes files by filename prefix:
# MAGIC
# MAGIC | Prefix | Source value written to bronze |
# MAGIC |---|---|
# MAGIC | `dane_*.{xlsx,xls,csv}` | `dane` |
# MAGIC | `policia_*.{xlsx,xls,csv}` | `policia_nacional` |
# MAGIC
# MAGIC The year is extracted from the first four-digit token in the filename
# MAGIC (`2024` in `policia_homicidios_2024.xlsx`).
# MAGIC
# MAGIC ### 5. If nothing is staged
# MAGIC
# MAGIC Running with an empty volume directory is allowed: the notebook logs
# MAGIC `no crime files staged` and exits successfully.

# COMMAND ----------

# DBTITLE 1,Imports specific to this notebook
import os
import re
import uuid
from datetime import datetime, timezone

import pandas as pd

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# COMMAND ----------

# DBTITLE 1,Config
PIPELINE_NAME = "05_ingest_crime_co"

# Volume directory where the operator drops raw DANE / Policía files.
RAW_DIR = "/Volumes/realestate/bronze/raw/crime_co"

# Target table.
TARGET_TABLE = "realestate.bronze.crime_co"

# Cities of interest — Bogotá (Cundinamarca / Distrito Capital) and Medellín (Antioquia).
# Keys are city names as we write them to bronze; values are the set of strings
# we'll accept as a match in the raw municipio/ciudad column (lowercase, accent-stripped).
CITY_FILTERS = {
    "Bogota": {
        "city_aliases": {"bogota", "bogota d.c.", "bogota dc", "bogota d c", "santa fe de bogota"},
        "departamento": "Cundinamarca",
        "departamento_aliases": {"cundinamarca", "bogota d.c.", "bogota dc", "bogota d c", "distrito capital"},
    },
    "Medellin": {
        "city_aliases": {"medellin"},
        "departamento": "Antioquia",
        "departamento_aliases": {"antioquia"},
    },
}

# Spanish month names → month number.
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

# COMMAND ----------

# DBTITLE 1,Text normalization + crime category mapping
def _strip_accents(text: str) -> str:
    """Lower-case + strip Spanish accents for fuzzy column / value matching."""
    if text is None:
        return ""
    s = str(text).lower().strip()
    replacements = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "ü": "u"}
    for src, dst in replacements.items():
        s = s.replace(src, dst)
    return s


def normalize_crime_category(crime_type: str) -> str:
    """
    Bucket a raw Spanish crime label into `violent` / `property` / `other`.

    - violent: homicidio, lesiones personales, secuestro, hurto a personas
      with violence ("violento" / "con violencia"), violencia intrafamiliar.
    - property: hurto, robo, daño en bien ajeno, abigeato (non-violent).
    - other: everything else (extorsión narcotráfico, drogas, etc.).
    """
    if not crime_type:
        return "other"

    raw = _strip_accents(crime_type)

    # Violent indicators first — order matters so violent hurto wins over plain hurto.
    violent_terms = (
        "homicid",
        "lesion",
        "secuestr",
        "violenc",
        "violacion",
        "feminicid",
        "delitos sexuales",
        "sexual",
    )
    if any(term in raw for term in violent_terms):
        return "violent"

    # "Hurto a personas" with explicit violence marker → violent. Plain hurto → property.
    if "hurto" in raw and ("violen" in raw or "con violencia" in raw or "violento" in raw):
        return "violent"

    property_terms = (
        "hurto",
        "robo",
        "dano",          # daño en bien ajeno (accent stripped)
        "abigeato",
        "piracy",
        "pirateria",
    )
    if any(term in raw for term in property_terms):
        return "property"

    return "other"

# COMMAND ----------

# DBTITLE 1,File discovery
def list_crime_files(raw_dir: str) -> list[dict]:
    """
    List Excel / CSV files staged in the volume. Returns one dict per file:
    `{path, name, source, year, ext}`. Skips dotfiles and hidden temp files.
    """
    if not os.path.isdir(raw_dir):
        print(f"Volume path does not exist: {raw_dir}")
        return []

    discovered = []
    for entry in sorted(os.listdir(raw_dir)):
        if entry.startswith("."):
            continue
        full_path = os.path.join(raw_dir, entry)
        if not os.path.isfile(full_path):
            continue

        ext = os.path.splitext(entry)[1].lower()
        if ext not in (".xlsx", ".xls", ".csv"):
            continue

        lower = entry.lower()
        if lower.startswith("dane"):
            source = "dane"
        elif lower.startswith("policia"):
            source = "policia_nacional"
        else:
            print(f"  Skipping unrecognized filename (must start with dane_ or policia_): {entry}")
            continue

        year_match = re.search(r"(20\d{2})", entry)
        if not year_match:
            print(f"  Skipping file without 4-digit year in name: {entry}")
            continue
        year = int(year_match.group(1))

        discovered.append({
            "path": full_path,
            "name": entry,
            "source": source,
            "year": year,
            "ext": ext,
        })

    return discovered


def read_tabular(path: str, ext: str) -> pd.DataFrame:
    """Read CSV or Excel into a pandas DataFrame, stringifying everything."""
    if ext == ".csv":
        # DANE CSVs are commonly UTF-8 with `;` separators. Fall back to `,` if needed.
        try:
            df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8")
            if df.shape[1] == 1:
                df = pd.read_csv(path, sep=",", dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=";", dtype=str, encoding="latin-1")
        return df
    # Excel — openpyxl handles .xlsx; pandas dispatches automatically.
    return pd.read_excel(path, dtype=str, engine="openpyxl" if ext == ".xlsx" else None)

# COMMAND ----------

# DBTITLE 1,Column resolution helpers (raw → semantic)
def _find_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    """Return the first column whose normalized name contains any candidate substring."""
    for col in df.columns:
        norm = _strip_accents(col)
        for cand in candidates:
            if cand in norm:
                return col
    return None


def _matches_filter(value: str, alias_set: set[str]) -> bool:
    return _strip_accents(value) in alias_set


def _resolve_city(row_city: str, row_dept: str) -> tuple[str | None, str | None, str | None]:
    """
    Decide which of our two target cities this row belongs to (if any).
    Returns (city, municipio, departamento) or (None, None, None) to drop the row.
    """
    norm_city = _strip_accents(row_city)
    norm_dept = _strip_accents(row_dept)

    for city_name, cfg in CITY_FILTERS.items():
        if (norm_city in cfg["city_aliases"]
                or (norm_dept in cfg["departamento_aliases"] and norm_city == _strip_accents(city_name))):
            return city_name, str(row_city).strip() if row_city else city_name, cfg["departamento"]

    # Bogotá in some files appears with municipio="BOGOTA D.C." and departamento="BOGOTA D.C."
    if norm_dept in CITY_FILTERS["Bogota"]["departamento_aliases"] and not norm_city:
        return "Bogota", "Bogota D.C.", "Cundinamarca"

    return None, None, None


def _parse_count(value) -> int | None:
    """Best-effort parse of an integer count. Returns None if unparseable."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    # Strip thousands separators (Colombian convention uses `.`).
    s_clean = s.replace(",", "").replace(".", "").replace(" ", "")
    if not s_clean.isdigit():
        return None
    return int(s_clean)


def _parse_month(value) -> int | None:
    """Parse a month cell that may be a name ('enero'), number ('01'), or already int."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        n = int(value)
        return n if 1 <= n <= 12 else None
    s = _strip_accents(value)
    if not s:
        return None
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 12 else None
    return SPANISH_MONTHS.get(s)

# COMMAND ----------

# DBTITLE 1,DANE parser
def parse_dane_file(path: str, ext: str, year: int) -> list[dict]:
    """
    Parse a DANE annual crime-by-municipio export. DANE files typically have a
    wide layout with one row per municipio and one column per crime type, or
    a long layout with separate columns for municipio / departamento /
    crime_type / count / month.

    The parser auto-detects layout by inspecting the columns.
    Returns a list of dicts ready for the bronze schema.
    """
    df = read_tabular(path, ext)
    if df.empty:
        return []

    # Locate semantic columns.
    municipio_col = _find_column(df, ("municipio", "ciudad", "municipios"))
    departamento_col = _find_column(df, ("departamento", "depto"))
    crime_col = _find_column(df, ("tipo de delito", "delito", "modalidad", "crimen", "tipo"))
    count_col = _find_column(df, ("conteo", "casos", "cantidad", "total", "victimas", "numero"))
    month_col = _find_column(df, ("mes", "periodo_mes"))
    year_col = _find_column(df, ("ano", "anio", "year", "periodo_ano"))

    rows: list[dict] = []

    # ---- Long layout: explicit crime_type + count columns ----
    if crime_col and count_col and municipio_col:
        for _, raw_row in df.iterrows():
            city, municipio, departamento = _resolve_city(
                raw_row.get(municipio_col), raw_row.get(departamento_col) if departamento_col else "",
            )
            if not city:
                continue

            crime_type = (raw_row.get(crime_col) or "").strip() if isinstance(raw_row.get(crime_col), str) else raw_row.get(crime_col)
            if not crime_type:
                continue

            count = _parse_count(raw_row.get(count_col))
            if count is None:
                continue

            period_month = _parse_month(raw_row.get(month_col)) if month_col else None
            period_year = year
            if year_col:
                yc = _parse_count(raw_row.get(year_col))
                if yc and 1900 < yc < 2100:
                    period_year = yc

            rows.append({
                "record_id": str(uuid.uuid4()),
                "source": "dane",
                "city": city,
                "municipio": municipio,
                "departamento": departamento,
                "crime_type": str(crime_type).strip(),
                "crime_category": normalize_crime_category(str(crime_type)),
                "count": int(count),
                "period_year": int(period_year),
                "period_month": int(period_month) if period_month else None,
            })
        return rows

    # ---- Wide layout: one column per crime type, one row per municipio ----
    if municipio_col:
        # Treat every non-key, non-text column as a candidate crime-type counter.
        skip_cols = {municipio_col}
        if departamento_col:
            skip_cols.add(departamento_col)
        candidate_crime_cols = [c for c in df.columns if c not in skip_cols]

        for _, raw_row in df.iterrows():
            city, municipio, departamento = _resolve_city(
                raw_row.get(municipio_col), raw_row.get(departamento_col) if departamento_col else "",
            )
            if not city:
                continue

            for crime_type in candidate_crime_cols:
                count = _parse_count(raw_row.get(crime_type))
                if count is None or count == 0:
                    continue
                rows.append({
                    "record_id": str(uuid.uuid4()),
                    "source": "dane",
                    "city": city,
                    "municipio": municipio,
                    "departamento": departamento,
                    "crime_type": str(crime_type).strip(),
                    "crime_category": normalize_crime_category(str(crime_type)),
                    "count": int(count),
                    "period_year": int(year),
                    "period_month": None,
                })
        return rows

    print(f"  WARN: could not detect schema for DANE file {os.path.basename(path)} — skipping")
    return []

# COMMAND ----------

# DBTITLE 1,Policia Nacional parser
def parse_policia_file(path: str, ext: str, year: int, name: str) -> list[dict]:
    """
    Parse a Policía Nacional SIEDCO export. SIEDCO files are usually one file
    per crime category — the category is inferred from the filename when the
    file itself doesn't expose a crime-type column. Months appear either as
    explicit rows or as one column per month.

    Returns a list of dicts ready for the bronze schema.
    """
    df = read_tabular(path, ext)
    if df.empty:
        return []

    municipio_col = _find_column(df, ("municipio", "ciudad"))
    departamento_col = _find_column(df, ("departamento", "depto"))
    crime_col = _find_column(df, ("delito", "tipo de delito", "modalidad", "conducta"))
    count_col = _find_column(df, ("casos", "cantidad", "total", "conteo", "victimas", "numero"))
    month_col = _find_column(df, ("mes", "periodo_mes"))
    year_col = _find_column(df, ("ano", "anio", "year"))

    # If filename carries a category and the file has no explicit crime column,
    # use the slug as crime_type.
    fname_slug = re.sub(r"^policia[_\-\s]+", "", _strip_accents(os.path.splitext(name)[0]))
    fname_slug = re.sub(r"\d+", "", fname_slug).strip("_-. ").replace("_", " ").strip()
    inferred_crime_from_filename = fname_slug or "delito"

    rows: list[dict] = []

    # ---- Long layout: explicit count column ----
    if municipio_col and count_col:
        for _, raw_row in df.iterrows():
            city, municipio, departamento = _resolve_city(
                raw_row.get(municipio_col), raw_row.get(departamento_col) if departamento_col else "",
            )
            if not city:
                continue

            crime_type_val = raw_row.get(crime_col) if crime_col else None
            crime_type = (str(crime_type_val).strip()
                          if crime_type_val and str(crime_type_val).strip()
                          else inferred_crime_from_filename)

            count = _parse_count(raw_row.get(count_col))
            if count is None:
                continue

            period_month = _parse_month(raw_row.get(month_col)) if month_col else None
            period_year = year
            if year_col:
                yc = _parse_count(raw_row.get(year_col))
                if yc and 1900 < yc < 2100:
                    period_year = yc

            rows.append({
                "record_id": str(uuid.uuid4()),
                "source": "policia_nacional",
                "city": city,
                "municipio": municipio,
                "departamento": departamento,
                "crime_type": crime_type,
                "crime_category": normalize_crime_category(crime_type),
                "count": int(count),
                "period_year": int(period_year),
                "period_month": int(period_month) if period_month else None,
            })
        return rows

    # ---- Wide layout: one column per month ----
    if municipio_col:
        month_columns: dict[str, int] = {}
        for col in df.columns:
            month_num = _parse_month(col)
            if month_num is not None:
                month_columns[col] = month_num

        if month_columns:
            for _, raw_row in df.iterrows():
                city, municipio, departamento = _resolve_city(
                    raw_row.get(municipio_col), raw_row.get(departamento_col) if departamento_col else "",
                )
                if not city:
                    continue

                crime_type_val = raw_row.get(crime_col) if crime_col else None
                crime_type = (str(crime_type_val).strip()
                              if crime_type_val and str(crime_type_val).strip()
                              else inferred_crime_from_filename)

                for col, month_num in month_columns.items():
                    count = _parse_count(raw_row.get(col))
                    if count is None or count == 0:
                        continue
                    rows.append({
                        "record_id": str(uuid.uuid4()),
                        "source": "policia_nacional",
                        "city": city,
                        "municipio": municipio,
                        "departamento": departamento,
                        "crime_type": crime_type,
                        "crime_category": normalize_crime_category(crime_type),
                        "count": int(count),
                        "period_year": int(year),
                        "period_month": int(month_num),
                    })
            return rows

    print(f"  WARN: could not detect schema for Policía file {name} — skipping")
    return []

# COMMAND ----------

# DBTITLE 1,Overwrite-by-(year, source) write helper
RESULT_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source", StringType(), True),
    StructField("city", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("crime_type", StringType(), True),
    StructField("crime_category", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("period_year", IntegerType(), True),
    StructField("period_month", IntegerType(), True),
    StructField("ingested_at", TimestampType(), False),
])


def write_rows_for_partition(rows: list[dict], source: str, year: int) -> int:
    """
    Overwrite the (year, source) partition: delete existing rows then insert.
    Returns the number of rows written.
    """
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    for r in rows:
        r["ingested_at"] = now

    df = spark.createDataFrame(rows, schema=RESULT_SCHEMA)

    # Idempotent overwrite for (period_year, source). Done in two steps because
    # MERGE without source data on the right would be more code for no benefit.
    spark.sql(
        f"DELETE FROM {TARGET_TABLE} "
        f"WHERE period_year = {int(year)} AND source = '{source}'"
    )
    df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    return df.count()

# COMMAND ----------

# DBTITLE 1,Run — discover, parse, and write
files = list_crime_files(RAW_DIR)

if not files:
    print(f"WARN: no crime files staged in {RAW_DIR} — see operator instructions above.")
    print("Notebook completing successfully with zero rows written.")
    summary = []
else:
    print(f"Found {len(files)} crime file(s) to process:")
    for f in files:
        print(f"  - {f['name']}  (source={f['source']}, year={f['year']})")

    summary = []
    for f in files:
        print(f"\nProcessing {f['name']} ...")
        try:
            if f["source"] == "dane":
                parsed = parse_dane_file(f["path"], f["ext"], f["year"])
            else:
                parsed = parse_policia_file(f["path"], f["ext"], f["year"], f["name"])

            written = write_rows_for_partition(parsed, f["source"], f["year"])
            print(f"  Parsed {len(parsed)} rows for Bogota/Medellin; wrote {written} to {TARGET_TABLE}")
            summary.append({
                "file": f["name"],
                "source": f["source"],
                "year": f["year"],
                "rows_written": written,
                "status": "SUCCESS",
                "error": "",
            })
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            print(f"  FAILED: {err}")
            summary.append({
                "file": f["name"],
                "source": f["source"],
                "year": f["year"],
                "rows_written": 0,
                "status": "FAILED",
                "error": err,
            })

# COMMAND ----------

# DBTITLE 1,Run summary
if summary:
    summary_df = spark.createDataFrame(summary)
    display(summary_df.orderBy("source", "year", "file"))
else:
    print("No files processed this run.")

# COMMAND ----------

# DBTITLE 1,Sanity counts from bronze.crime_co
display(
    spark.sql(f"""
        SELECT
            source,
            city,
            period_year,
            crime_category,
            SUM(count) AS total_count,
            COUNT(*)   AS row_count
        FROM {TARGET_TABLE}
        GROUP BY source, city, period_year, crime_category
        ORDER BY source, city, period_year, crime_category
    """)
)

# COMMAND ----------

# DBTITLE 1,Most recent rows (preview)
display(
    spark.sql(f"""
        SELECT
            record_id,
            source,
            city,
            municipio,
            departamento,
            crime_type,
            crime_category,
            count,
            period_year,
            period_month,
            ingested_at
        FROM {TARGET_TABLE}
        ORDER BY ingested_at DESC, source, city, crime_type
        LIMIT 50
    """)
)
