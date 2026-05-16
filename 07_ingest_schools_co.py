# Databricks notebook source
# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Real Estate Agent - Ingest Colombia Schools (ICFES + MEN)
# MAGIC %md
# MAGIC # 07 — Ingest Colombia schools (ICFES Saber 11 + MEN registry)
# MAGIC
# MAGIC Loads Colombian school data from two bulk files that an operator drops into the
# MAGIC `realestate.bronze.raw` volume. **No live API call is made** — Colombia's open data
# MAGIC portals require interactive search/download, so this notebook is a parser, not a
# MAGIC scraper.
# MAGIC
# MAGIC **Sources:**
# MAGIC 1. **ICFES Saber 11** — annual nation-wide standardized test, one row per student.
# MAGIC    Aggregated here to a per-institution average score and converted to a national
# MAGIC    percentile (0–100, higher = better).
# MAGIC 2. **MEN school registry** (Ministerio de Educación Nacional) — provides
# MAGIC    institution metadata: city, municipio, departamento, lat/lon, grade levels,
# MAGIC    enrollment, school type (`oficial` vs `privado`).
# MAGIC
# MAGIC The two files are joined on the institution DANE/MEN code. Output is filtered to
# MAGIC Bogotá and Medellín only and upserted to `realestate.bronze.schools_co` using
# MAGIC `school_id` as the merge key.
# MAGIC
# MAGIC **Schedule:** Annual (ICFES results are published once per year).

# COMMAND ----------

# MAGIC %run ./99_helpers

# COMMAND ----------

# DBTITLE 1,Use catalog
spark.sql("USE CATALOG realestate")

# COMMAND ----------

# DBTITLE 1,Operator instructions
# MAGIC %md
# MAGIC ## Operator: how to refresh the source files
# MAGIC
# MAGIC This notebook reads two files from the volume
# MAGIC `/Volumes/realestate/bronze/raw/schools_co/`. Drop the latest versions there
# MAGIC before running. Either CSV or Excel (`.xlsx`) is accepted for both files.
# MAGIC
# MAGIC ### 1. ICFES Saber 11 annual results
# MAGIC
# MAGIC 1. Open <https://www.icfes.gov.co/resultados> (or the current "Datos Abiertos
# MAGIC    ICFES" portal — the URL changes annually).
# MAGIC 2. Find the most recent "Resultados Saber 11 — Bases de Datos" bulk download.
# MAGIC 3. Download the CSV (or Excel) of individual-student results. The file is large
# MAGIC    (~hundreds of MB) and contains one row per test-taker nationwide.
# MAGIC 4. Required columns (names vary by year; the parser below tries common variants):
# MAGIC    * Institution code: `cod_inst`, `cole_cod_dane_institucion`, `codigo_dane`, …
# MAGIC    * Global score: `punt_global`, `puntaje_global`, …
# MAGIC    * Year: `periodo` or `anio` (else the filename year is used).
# MAGIC 5. Save as `icfes_saber11_<year>.csv` (or `.xlsx`) in the volume folder above.
# MAGIC
# MAGIC ### 2. MEN school registry
# MAGIC
# MAGIC 1. Open <https://www.datos.gov.co> and search for **"directorio establecimientos
# MAGIC    educativos"** (Ministerio de Educación Nacional dataset).
# MAGIC 2. Export the full dataset as CSV (or Excel).
# MAGIC 3. Required columns (parser tries common variants):
# MAGIC    * Institution code: `codigo_dane`, `codigo_dane_sede`, `cod_inst`, …
# MAGIC    * `nombre_establecimiento` / `institucion`
# MAGIC    * `municipio`, `departamento`
# MAGIC    * `latitud`, `longitud`
# MAGIC    * `niveles` / `grados` (grade levels)
# MAGIC    * `matricula` / `total_estudiantes` (enrollment)
# MAGIC    * `sector` (`oficial` vs `no oficial` / `privado`)
# MAGIC 4. Save as `men_directorio_<year>.csv` (or `.xlsx`) in the volume folder above.
# MAGIC
# MAGIC ### File naming convention
# MAGIC
# MAGIC The parser picks files by substring match on the filename:
# MAGIC * `icfes` and/or `saber11` → ICFES file
# MAGIC * `men`, `directorio`, or `establecimientos` → MEN file
# MAGIC
# MAGIC If the volume folder is empty the notebook logs a warning and exits cleanly.

# COMMAND ----------

# DBTITLE 1,Imports and config
import os
import re
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Volume folder where the operator drops the bulk files.
RAW_VOLUME_DIR = "/Volumes/realestate/bronze/raw/schools_co"

# Target Delta table.
TARGET_TABLE = "realestate.bronze.schools_co"

# Filename substring rules for picking the two source files out of the volume folder.
ICFES_FILENAME_HINTS = ("icfes", "saber11", "saber_11", "saber-11")
MEN_FILENAME_HINTS = ("men", "directorio", "establecimientos")

# Candidate column names for each logical field. Lowercased, accents stripped, and
# non-alphanumerics dropped before matching, so e.g. "Código DANE" and "codigo_dane"
# both reduce to "codigodane".
ICFES_INSTITUTION_CODE_CANDIDATES = (
    "codinst",
    "codigodane",
    "codigodaneinstitucion",
    "colecoddaneinstitucion",
    "colecoddane",
    "coleinstcoddane",
    "codigoestablecimiento",
)
ICFES_SCORE_CANDIDATES = (
    "puntglobal",
    "puntajeglobal",
    "puntajetotal",
    "punttotal",
)
ICFES_YEAR_CANDIDATES = ("periodo", "anio", "ano", "year")

MEN_INSTITUTION_CODE_CANDIDATES = (
    "codigodane",
    "codigodanesede",
    "codinst",
    "codigoestablecimiento",
    "codigoinstitucion",
)
MEN_NAME_CANDIDATES = (
    "nombreestablecimiento",
    "nombreinstitucion",
    "institucion",
    "establecimiento",
    "nombresede",
)
MEN_CITY_CANDIDATES = ("ciudad", "municipio")
MEN_MUNICIPIO_CANDIDATES = ("municipio",)
MEN_DEPARTAMENTO_CANDIDATES = ("departamento", "depto")
MEN_LAT_CANDIDATES = ("latitud", "lat", "latitude")
MEN_LON_CANDIDATES = ("longitud", "lon", "lng", "longitude")
MEN_GRADES_CANDIDATES = (
    "niveles",
    "grados",
    "nivelesofrecidos",
    "gradosofrecidos",
    "nivel",
)
MEN_ENROLLMENT_CANDIDATES = (
    "matricula",
    "totalestudiantes",
    "totalmatriculados",
    "numerodeestudiantes",
    "matriculatotal",
)
MEN_SECTOR_CANDIDATES = ("sector", "tipoestablecimiento", "naturaleza")

# Departamentos / municipios for the two target cities. Bogotá is a Capital District
# (`Bogotá D.C.`) but some datasets still list `Cundinamarca`; accept both.
BOGOTA_DEPARTAMENTOS = {"bogota dc", "bogota d c", "bogota distrito capital", "cundinamarca"}
MEDELLIN_DEPARTAMENTOS = {"antioquia"}
BOGOTA_MUNICIPIOS = {"bogota", "bogota dc", "bogota d c"}
MEDELLIN_MUNICIPIOS = {"medellin"}

print(f"Raw volume     : {RAW_VOLUME_DIR}")
print(f"Target table   : {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,File discovery
def _normalize_token(value: str) -> str:
    """Lowercase, strip accents, drop non-alphanumerics. Used to match column / file names."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    # Strip common Spanish accents.
    s = (
        s.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return re.sub(r"[^a-z0-9]", "", s)


def _list_volume_files(volume_dir: str) -> list[str]:
    """Return absolute paths of all files in a volume directory (non-recursive)."""
    try:
        entries = dbutils.fs.ls(volume_dir)  # noqa: F821 - dbutils provided by Databricks runtime
    except Exception as exc:
        print(f"Could not list volume {volume_dir}: {exc}")
        return []
    paths = []
    for entry in entries:
        # dbutils returns dbfs paths; the Python file APIs use the same /Volumes path.
        # Skip subdirectories.
        if entry.size == 0 and entry.path.endswith("/"):
            continue
        paths.append(entry.path)
    return paths


def _pick_file(paths: list[str], hints: tuple[str, ...]) -> Optional[str]:
    """Return the first path whose normalized basename contains any of the hints."""
    for path in paths:
        base_norm = _normalize_token(os.path.basename(path))
        for hint in hints:
            if hint in base_norm:
                return path
    return None


all_files = _list_volume_files(RAW_VOLUME_DIR)
icfes_path = _pick_file(all_files, ICFES_FILENAME_HINTS)
men_path = _pick_file(all_files, MEN_FILENAME_HINTS)

print(f"Files in volume: {len(all_files)}")
for p in all_files:
    print(f"  - {p}")
print(f"ICFES file     : {icfes_path}")
print(f"MEN file       : {men_path}")

if not all_files or icfes_path is None or men_path is None:
    print(
        "WARNING: Required source files are missing from "
        f"{RAW_VOLUME_DIR}. See the operator instructions cell above. "
        "Skipping ingest — completing successfully with no upsert."
    )
    dbutils.notebook.exit("skipped: no source files")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Generic file reader (CSV or Excel)
def _read_tabular(path: str) -> pd.DataFrame:
    """
    Read a CSV or Excel file from a Unity Catalog volume into a pandas DataFrame.

    Tries multiple encodings/delimiters because Colombian government datasets are
    inconsistent: ICFES historically ships as `;`-delimited Latin-1 CSV, while MEN
    sometimes ships UTF-8 with `,` and sometimes Excel.
    """
    base = os.path.basename(path).lower()
    if base.endswith((".xlsx", ".xls")):
        return pd.read_excel(path, dtype=str)

    last_err = None
    for encoding in ("utf-8", "latin-1"):
        for sep in (",", ";", "|", "\t"):
            try:
                df = pd.read_csv(path, dtype=str, encoding=encoding, sep=sep, low_memory=False)
                # A successful parse should produce at least 2 columns; otherwise the
                # delimiter is wrong.
                if df.shape[1] >= 2:
                    return df
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                continue
    raise RuntimeError(f"Could not parse {path}: {last_err}")


def _resolve_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> Optional[str]:
    """Return the actual DataFrame column whose normalized name is in `candidates`."""
    norm_map = {_normalize_token(c): c for c in df.columns}
    for cand in candidates:
        if cand in norm_map:
            return norm_map[cand]
    return None


def _year_from_filename(path: str) -> Optional[int]:
    """Extract the first 4-digit year (2000-2099) from a filename, if any."""
    match = re.search(r"(20\d{2})", os.path.basename(path))
    return int(match.group(1)) if match else None


# COMMAND ----------

# DBTITLE 1,Parse ICFES results -> per-institution average + national percentile
print(f"Reading ICFES file: {icfes_path}")
icfes_df = _read_tabular(icfes_path)
print(f"  rows: {len(icfes_df):,}  cols: {icfes_df.shape[1]}")

icfes_code_col = _resolve_column(icfes_df, ICFES_INSTITUTION_CODE_CANDIDATES)
icfes_score_col = _resolve_column(icfes_df, ICFES_SCORE_CANDIDATES)
icfes_year_col = _resolve_column(icfes_df, ICFES_YEAR_CANDIDATES)

if icfes_code_col is None or icfes_score_col is None:
    raise RuntimeError(
        f"ICFES file is missing required columns. "
        f"Found columns: {list(icfes_df.columns)[:30]} ... "
        f"Need an institution code (one of {ICFES_INSTITUTION_CODE_CANDIDATES}) "
        f"and a score (one of {ICFES_SCORE_CANDIDATES})."
    )

print(f"  using code column : {icfes_code_col}")
print(f"  using score column: {icfes_score_col}")
print(f"  using year column : {icfes_year_col}")

# Coerce score to numeric; drop rows with missing code or score.
icfes_df["_score_num"] = pd.to_numeric(icfes_df[icfes_score_col], errors="coerce")
icfes_df["_code_str"] = icfes_df[icfes_code_col].astype(str).str.strip()
icfes_df = icfes_df[(icfes_df["_code_str"] != "") & (icfes_df["_score_num"].notna())]

# Determine reporting year. Prefer an in-file column; fall back to a year in the
# filename; finally fall back to the current calendar year.
if icfes_year_col is not None:
    icfes_df["_year_int"] = pd.to_numeric(icfes_df[icfes_year_col].astype(str).str[:4], errors="coerce")
    year_mode = icfes_df["_year_int"].dropna().mode()
    icfes_year = int(year_mode.iloc[0]) if not year_mode.empty else None
else:
    icfes_year = None
if icfes_year is None:
    icfes_year = _year_from_filename(icfes_path) or datetime.utcnow().year
print(f"  reporting year    : {icfes_year}")

# Aggregate to per-institution average across all students nationwide. Percentile is
# computed nationally BEFORE filtering to Bogotá / Medellín so the rank reflects
# Colombia-wide standing.
icfes_agg = (
    icfes_df.groupby("_code_str", as_index=False)
    .agg(icfes_score=("_score_num", "mean"), student_count=("_score_num", "size"))
    .rename(columns={"_code_str": "school_id"})
)
# Rank ascending so higher score => higher percentile. Use average ties for stability.
ranks = icfes_agg["icfes_score"].rank(method="average", ascending=True)
n_inst = len(icfes_agg)
if n_inst > 1:
    icfes_agg["icfes_percentile"] = (ranks - 1) / (n_inst - 1) * 100.0
else:
    icfes_agg["icfes_percentile"] = 100.0
icfes_agg["icfes_year"] = icfes_year

print(f"  institutions ranked: {n_inst:,}")
print(icfes_agg.head(5).to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Parse MEN registry
print(f"Reading MEN file: {men_path}")
men_df = _read_tabular(men_path)
print(f"  rows: {len(men_df):,}  cols: {men_df.shape[1]}")

men_code_col = _resolve_column(men_df, MEN_INSTITUTION_CODE_CANDIDATES)
men_name_col = _resolve_column(men_df, MEN_NAME_CANDIDATES)
men_city_col = _resolve_column(men_df, MEN_CITY_CANDIDATES)
men_muni_col = _resolve_column(men_df, MEN_MUNICIPIO_CANDIDATES)
men_dept_col = _resolve_column(men_df, MEN_DEPARTAMENTO_CANDIDATES)
men_lat_col = _resolve_column(men_df, MEN_LAT_CANDIDATES)
men_lon_col = _resolve_column(men_df, MEN_LON_CANDIDATES)
men_grades_col = _resolve_column(men_df, MEN_GRADES_CANDIDATES)
men_enroll_col = _resolve_column(men_df, MEN_ENROLLMENT_CANDIDATES)
men_sector_col = _resolve_column(men_df, MEN_SECTOR_CANDIDATES)

if men_code_col is None:
    raise RuntimeError(
        f"MEN file is missing an institution code column. Found: {list(men_df.columns)[:30]} ... "
        f"Need one of {MEN_INSTITUTION_CODE_CANDIDATES}."
    )

print(f"  using code column      : {men_code_col}")
print(f"  using name column      : {men_name_col}")
print(f"  using municipio column : {men_muni_col}")
print(f"  using departamento col : {men_dept_col}")
print(f"  using lat / lon cols   : {men_lat_col} / {men_lon_col}")
print(f"  using grades column    : {men_grades_col}")
print(f"  using enrollment col   : {men_enroll_col}")
print(f"  using sector column    : {men_sector_col}")


def _sector_to_school_type(raw: Optional[str]) -> Optional[str]:
    """
    Map a MEN `sector` value to the canonical bronze enum: `oficial` or `privado`.
    """
    if raw is None:
        return None
    token = _normalize_token(raw)
    if token == "":
        return None
    # "oficial" / "publico" -> oficial; "no oficial" / "privado" -> privado.
    if "nooficial" in token or "privad" in token or "particular" in token:
        return "privado"
    if "oficial" in token or "publico" in token or "estatal" in token:
        return "oficial"
    return None


# Build the MEN-side projection with stable canonical column names.
men_out = pd.DataFrame()
men_out["school_id"] = men_df[men_code_col].astype(str).str.strip()
men_out["institution_name"] = men_df[men_name_col].astype(str).str.strip() if men_name_col else None
men_out["municipio"] = men_df[men_muni_col].astype(str).str.strip() if men_muni_col else None
men_out["departamento"] = men_df[men_dept_col].astype(str).str.strip() if men_dept_col else None
men_out["city"] = (
    men_df[men_city_col].astype(str).str.strip()
    if men_city_col and men_city_col != men_muni_col
    else men_out["municipio"]
)
men_out["lat"] = pd.to_numeric(men_df[men_lat_col], errors="coerce") if men_lat_col else None
men_out["lon"] = pd.to_numeric(men_df[men_lon_col], errors="coerce") if men_lon_col else None
men_out["grade_levels"] = men_df[men_grades_col].astype(str).str.strip() if men_grades_col else None
men_out["enrollment"] = (
    pd.to_numeric(men_df[men_enroll_col], errors="coerce").astype("Int64") if men_enroll_col else pd.NA
)
men_out["school_type"] = (
    men_df[men_sector_col].map(_sector_to_school_type) if men_sector_col else None
)

# Drop rows without an institution code; deduplicate so the merge join is 1:1.
men_out = men_out[men_out["school_id"] != ""].drop_duplicates(subset=["school_id"], keep="first")
print(f"  unique institutions in MEN: {len(men_out):,}")

# COMMAND ----------

# DBTITLE 1,Join ICFES <-> MEN and filter to Bogota / Medellin
joined = men_out.merge(icfes_agg, on="school_id", how="inner")
print(f"Joined rows (MEN inner ICFES): {len(joined):,}")

# Normalize departamento / municipio for filtering.
joined["_dept_norm"] = joined["departamento"].map(_normalize_token)
joined["_muni_norm"] = joined["municipio"].map(_normalize_token)

bogota_dept_norm = {_normalize_token(s) for s in BOGOTA_DEPARTAMENTOS}
medellin_dept_norm = {_normalize_token(s) for s in MEDELLIN_DEPARTAMENTOS}
bogota_muni_norm = {_normalize_token(s) for s in BOGOTA_MUNICIPIOS}
medellin_muni_norm = {_normalize_token(s) for s in MEDELLIN_MUNICIPIOS}

is_bogota = joined["_dept_norm"].isin(bogota_dept_norm) & joined["_muni_norm"].isin(bogota_muni_norm)
is_medellin = joined["_dept_norm"].isin(medellin_dept_norm) & joined["_muni_norm"].isin(medellin_muni_norm)
filtered = joined[is_bogota | is_medellin].copy()
filtered = filtered.drop(columns=["_dept_norm", "_muni_norm"])

print(f"  Bogota rows  : {int(is_bogota.sum()):,}")
print(f"  Medellin rows: {int(is_medellin.sum()):,}")
print(f"  Total filtered: {len(filtered):,}")

if filtered.empty:
    print(
        "WARNING: Join produced zero Bogota / Medellin rows. "
        "Check that the MEN file covers both cities and the institution codes "
        "match between ICFES and MEN. Skipping upsert."
    )
    dbutils.notebook.exit("skipped: empty after filter")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Build Spark DataFrame matching bronze.schools_co
# Stage the pandas result into a Spark DataFrame with an explicit schema so the merge
# is type-stable regardless of NA handling in pandas.
schema = StructType(
    [
        StructField("school_id", StringType(), nullable=False),
        StructField("institution_name", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("municipio", StringType(), nullable=True),
        StructField("departamento", StringType(), nullable=True),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
        StructField("grade_levels", StringType(), nullable=True),
        StructField("enrollment", IntegerType(), nullable=True),
        StructField("school_type", StringType(), nullable=True),
        StructField("icfes_score", DoubleType(), nullable=True),
        StructField("icfes_percentile", DoubleType(), nullable=True),
        StructField("icfes_year", IntegerType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)


def _to_py(value, caster):
    """pandas NA -> None, otherwise cast via the given callable."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA:
        return None
    try:
        return caster(value)
    except (TypeError, ValueError):
        return None


now_ts = datetime.utcnow()
rows = []
for r in filtered.itertuples(index=False):
    rec = r._asdict()
    rows.append(
        (
            str(rec["school_id"]),
            _to_py(rec.get("institution_name"), str),
            _to_py(rec.get("city"), str),
            _to_py(rec.get("municipio"), str),
            _to_py(rec.get("departamento"), str),
            _to_py(rec.get("lat"), float),
            _to_py(rec.get("lon"), float),
            _to_py(rec.get("grade_levels"), str),
            _to_py(rec.get("enrollment"), int),
            _to_py(rec.get("school_type"), str),
            _to_py(rec.get("icfes_score"), float),
            _to_py(rec.get("icfes_percentile"), float),
            _to_py(rec.get("icfes_year"), int),
            now_ts,
        )
    )

staged_df = spark.createDataFrame(rows, schema=schema)
staged_df.createOrReplaceTempView("schools_co_staging")
print(f"Staged rows ready for upsert: {staged_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Upsert to bronze.schools_co (merge on school_id)
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS target
    USING schools_co_staging AS source
    ON target.school_id = source.school_id
    WHEN MATCHED THEN UPDATE SET
        institution_name = source.institution_name,
        city             = source.city,
        municipio        = source.municipio,
        departamento     = source.departamento,
        lat              = source.lat,
        lon              = source.lon,
        grade_levels     = source.grade_levels,
        enrollment       = source.enrollment,
        school_type      = source.school_type,
        icfes_score      = source.icfes_score,
        icfes_percentile = source.icfes_percentile,
        icfes_year       = source.icfes_year,
        ingested_at      = source.ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
)
print(f"Upsert complete into {TARGET_TABLE}.")

# COMMAND ----------

# DBTITLE 1,Counts and sanity-check summary
summary = spark.sql(
    f"""
    SELECT
        COUNT(*)                                     AS total_rows,
        COUNT(DISTINCT school_id)                    AS distinct_schools,
        SUM(CASE WHEN city ILIKE 'bogota%' THEN 1 ELSE 0 END) AS bogota_rows,
        SUM(CASE WHEN city ILIKE 'medellin%' THEN 1 ELSE 0 END) AS medellin_rows,
        SUM(CASE WHEN school_type = 'oficial' THEN 1 ELSE 0 END) AS oficial_rows,
        SUM(CASE WHEN school_type = 'privado' THEN 1 ELSE 0 END) AS privado_rows,
        ROUND(AVG(icfes_score),  2)                  AS avg_icfes_score,
        ROUND(AVG(icfes_percentile), 2)              AS avg_icfes_percentile,
        MAX(icfes_year)                              AS latest_icfes_year,
        MAX(ingested_at)                             AS last_ingest
    FROM {TARGET_TABLE}
    """
).first().asDict()

for k, v in summary.items():
    print(f"  {k:<22} {v}")

display(  # noqa: F821 - Databricks builtin
    spark.sql(
        f"""
        SELECT school_id, institution_name, city, school_type,
               ROUND(icfes_score, 2) AS icfes_score,
               ROUND(icfes_percentile, 2) AS icfes_percentile,
               icfes_year
        FROM {TARGET_TABLE}
        ORDER BY icfes_percentile DESC
        LIMIT 20
        """
    )
)
