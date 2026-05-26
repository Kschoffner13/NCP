# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze → Silver: Sites
# MAGIC Selects curated columns from `01_bronze.sites`, casts them to the correct types,
# MAGIC and writes the result to `02_silver.sites`.
# MAGIC
# MAGIC **Re-run safe:** silver table is fully replaced on every run.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

# `spark` and `dbutils` are injected by the Databricks runtime.

# COMMAND ----------

# MAGIC %md ### Config

# COMMAND ----------

CATALOG       = "network_capacity_planning"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
SOURCE_TABLE  = "sites"

BRONZE_REF = f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{SOURCE_TABLE}`"
SILVER_REF = f"`{CATALOG}`.`{SILVER_SCHEMA}`.`{SOURCE_TABLE}`"

# Column mapping: (bronze_col, silver_col, target_type)
#
# bronze_col  — sanitised name as it exists in the bronze table
# silver_col  — name to use in the silver table (usually the same)
# target_type — STRING keeps the value as-is; INT / DOUBLE trigger a cast
COLUMN_MAP = [
    ("site_id",           "site_id",           "INT"),
    ("site_code",         "site_code",         "STRING"),
    ("site_name",         "site_name",         "STRING"),
    ("network_name",      "network_name",      "STRING"),
    ("site_status",       "site_status",       "STRING"),
    ("latitude",          "latitude",          "DOUBLE"),
    ("longitude",         "longitude",         "DOUBLE"),
    ("long_term_plan",    "long_term_plan",    "STRING"),
    ("cx_count",          "cx_count",          "DOUBLE"),
    ("network_function",  "network_function",  "STRING"),
    ("province",          "province",          "STRING"),
    ("vre_owner_company", "vre_owner_company", "STRING"),
    ("vre_type",          "vre_type",          "STRING"),
    ("vre_description",   "vre_description",   "STRING"),
    ("total_fw_ucd",      "total_fw_used",     "DOUBLE"),
]

# COMMAND ----------

# MAGIC %md ### Read bronze & verify columns

# COMMAND ----------

df_raw = spark.table(BRONZE_REF)
bronze_cols = set(df_raw.columns)

missing = [b for b, _, _ in COLUMN_MAP if b not in bronze_cols]
if missing:
    print(f"WARNING — columns not found in bronze (skipped): {missing}")

valid_map = [(b, s, t) for b, s, t in COLUMN_MAP if b in bronze_cols]
print(f"Columns mapped : {len(valid_map)} of {len(COLUMN_MAP)}")

# COMMAND ----------

# MAGIC %md ### Build silver DataFrame

# COMMAND ----------

select_exprs = []
for bronze_col, silver_col, dtype in valid_map:
    col = F.trim(F.col(bronze_col))
    if dtype != "STRING":
        col = col.cast(dtype)
    select_exprs.append(col.alias(silver_col))

df_silver = df_raw.select(select_exprs)

# COMMAND ----------

# MAGIC %md ### Write silver table

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_REF)
)

silver_count = spark.table(SILVER_REF).count()
print(f"Written to {SILVER_REF} : {silver_count:,} rows")

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
print(f"Run completed : {run_time}")
print(f"Silver rows   : {silver_count:,}")
