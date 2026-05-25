# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw → Bronze Ingestion
# MAGIC Discovers every CSV in the `input_data` volume under `00_landing` and writes
# MAGIC it as a Delta table in `01_bronze`. Designed to run hourly as a Databricks Job.
# MAGIC
# MAGIC **Re-run safe:** each table is fully replaced on every run so schema changes
# MAGIC and new rows in the source file are always reflected.

# COMMAND ----------

import re
from datetime import datetime, timezone
from typing import Any

# Declared as Any so static analysis tools don't flag attribute access.
# The Databricks runtime replaces these with real objects at execution time.
spark:   Any = None
dbutils: Any = None

# ---------------------------------------------------------------------------
# Config — edit these to match your environment
# ---------------------------------------------------------------------------

CATALOG       = "NCP"
RAW_SCHEMA    = "00_landing"
BRONZE_SCHEMA = "01_bronze"
VOLUME        = "input_data"

VOLUME_PATH   = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"

# COMMAND ----------

# MAGIC %md ### Helpers

# COMMAND ----------

def _sanitize(name: str) -> str:
    """Convert an arbitrary string to a valid Delta column/table identifier."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    name = re.sub(r"_+", "_", name)
    if name and name[0].isdigit():
        name = "col_" + name
    return name or "col"


def _table_name(filename: str) -> str:
    """Derive a clean table name from a CSV filename.

    Strips Quickbase-style timestamp suffixes:
      'Indexed Site Lookup-data-2026-05-18 16_04_52.csv'  →  'indexed_site_lookup'
    """
    stem = filename
    if stem.lower().endswith(".csv"):
        stem = stem[:-4]
    stem = re.sub(r"[-_ ]data[-_ ]\d{4}.*$", "", stem, flags=re.IGNORECASE)
    return _sanitize(stem)


def _dedupe_columns(names: list) -> list:
    """Append a counter to any column name that collides after sanitisation."""
    seen = {}
    result = []
    for n in names:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            result.append(n)
    return result

# COMMAND ----------

# MAGIC %md ### Discovery

# COMMAND ----------

all_files = dbutils.fs.ls(VOLUME_PATH)
csv_files = [f for f in all_files if f.name.lower().endswith(".csv")]

print(f"Volume  : {VOLUME_PATH}")
print(f"Found   : {len(csv_files)} CSV file(s)\n")

for f in csv_files:
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md ### Ingestion

# COMMAND ----------

results = []

for file_info in csv_files:
    filename  = file_info.name
    file_path = file_info.path
    tbl_name  = _table_name(filename)
    tbl_ref   = f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{tbl_name}`"

    print(f"Processing : {filename}")
    print(f"Target     : {tbl_ref}")

    try:
        df = (
            spark.read
            .option("header",    "true")
            .option("inferSchema", "false")   # all columns land as STRING in bronze
            .option("multiLine", "true")       # handle newlines inside quoted fields
            .option("quote",     '"')
            .option("escape",    '"')
            .option("encoding",  "UTF-8")
            .csv(file_path)
        )

        # Sanitise column names
        raw_cols      = df.columns
        sanitized     = _dedupe_columns([_sanitize(c) for c in raw_cols])
        renamed_pairs = [(old, new) for old, new in zip(raw_cols, sanitized) if old != new]

        for old, new in renamed_pairs:
            df = df.withColumnRenamed(old, new)

        # Overwrite the table so schema changes in the source are always captured
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tbl_ref)
        )

        row_count = spark.table(tbl_ref).count()
        results.append({"file": filename, "table": tbl_ref, "rows": row_count, "status": "ok"})
        print(f"Loaded     : {row_count:,} rows  |  {len(sanitized)} columns")

    except Exception as e:
        results.append({"file": filename, "table": tbl_ref, "rows": 0, "status": str(e)})
        print(f"ERROR      : {e}")

    print()

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

run_time   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
n_ok       = sum(1 for r in results if r["status"] == "ok")
n_errors   = sum(1 for r in results if r["status"] != "ok")

print(f"Run completed : {run_time}")
print(f"Success : {n_ok}   Errors : {n_errors}\n")

for r in results:
    icon = "✓" if r["status"] == "ok" else "✗"
    print(f"  {icon}  {r['file']:<55}  {r['rows']:>8,} rows   {r['status']}")

if n_errors:
    raise Exception(f"{n_errors} file(s) failed to ingest — see output above for details.")
