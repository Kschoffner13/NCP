# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze → Silver: Sites
# MAGIC Selects curated columns from `01_bronze.sites`, casts them to the correct types,
# MAGIC and writes clean rows to `02_silver.sites`.
# MAGIC Validation errors are written to `02_silver.field_errors` for display in the UI.
# MAGIC
# MAGIC **Re-run safe:** silver table is fully replaced each run; field_errors is
# MAGIC overwritten only for the `sites` partition so other tables' errors are preserved.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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
ERRORS_REF = f"`{CATALOG}`.`{SILVER_SCHEMA}`.`field_errors`"

# ---------------------------------------------------------------------------
# Column mapping: (bronze_col, silver_col, target_type)
#
# bronze_col  — sanitised name as it exists in the bronze table
# silver_col  — name to use in the silver table (usually the same)
# target_type — STRING keeps the value as-is; INT / DOUBLE trigger a cast
#               and a cast-failure error if the raw value is non-empty but
#               cannot be converted
# ---------------------------------------------------------------------------
COLUMN_MAP = [
    ("site_id",           "site_id",           "INT"),
    ("site_code",         "site_code",         "STRING"),
    ("site_name",         "site_name",         "STRING"),
    ("network_name",      "network_name",       "STRING"),
    ("site_status",       "site_status",        "STRING"),
    ("latitude",          "latitude",           "DOUBLE"),
    ("longitude",         "longitude",          "DOUBLE"),
    ("long_term_plan",    "long_term_plan",     "STRING"),
    ("cx_count",          "cx_count",           "DOUBLE"),
    ("network_function",  "network_function",   "STRING"),
    ("province",          "province",           "STRING"),
    ("vre_owner_company", "vre_owner_company",  "STRING"),
    ("vre_type",          "vre_type",           "STRING"),
    ("vre_description",   "vre_description",    "STRING"),
    ("total_fw_ucd",      "total_fw_used",      "DOUBLE"),
]

# Fields that must be non-null and non-empty in every row
REQUIRED = {"site_id", "site_code", "site_name", "site_status"}

# Validation rules applied after casting
LAT_MIN,  LAT_MAX  =  41.7,  83.1
LON_MIN,  LON_MAX  = -141.0, -52.6
VALID_PROVINCES = {
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
}

# COMMAND ----------

# MAGIC %md ### Read bronze & verify columns

# COMMAND ----------

df_raw = spark.table(BRONZE_REF)
bronze_cols = set(df_raw.columns)

missing = [b for b, _, _ in COLUMN_MAP if b not in bronze_cols]
if missing:
    print(f"WARNING — requested columns not found in bronze (skipped): {missing}")

valid_map = [(b, s, t) for b, s, t in COLUMN_MAP if b in bronze_cols]
print(f"Columns mapped : {len(valid_map)} of {len(COLUMN_MAP)}")

# COMMAND ----------

# MAGIC %md ### Build silver DataFrame

# COMMAND ----------

# Cast / rename each column for silver
select_exprs = []
for bronze_col, silver_col, dtype in valid_map:
    col = F.trim(F.col(bronze_col))
    if dtype != "STRING":
        col = col.cast(dtype)
    select_exprs.append(col.alias(silver_col))

df_silver = df_raw.select(select_exprs)

# COMMAND ----------

# MAGIC %md ### Detect errors

# COMMAND ----------

ERROR_SCHEMA = StructType([
    StructField("source_table",  StringType(),   False),
    StructField("record_id",     StringType(),   True),
    StructField("field_name",    StringType(),   True),
    StructField("field_value",   StringType(),   True),
    StructField("error_type",    StringType(),   True),
    StructField("error_message", StringType(),   True),
    StructField("detected_at",   TimestampType(),True),
])

def _error_rows(base_df, field_name, raw_value_col, error_type, message):
    """Project a filtered DataFrame into the standard error schema."""
    return base_df.select(
        F.lit(SOURCE_TABLE).alias("source_table"),
        F.col("site_id").cast("string").alias("record_id"),
        F.lit(field_name).alias("field_name"),
        F.coalesce(F.col(raw_value_col).cast("string"), F.lit("")).alias("field_value"),
        F.lit(error_type).alias("error_type"),
        F.lit(message).alias("error_message"),
        F.current_timestamp().alias("detected_at"),
    )

error_frames = []

# 1. Null / empty required fields  (check raw STRING before casting)
df_raw_sel = df_raw.select([F.trim(F.col(b)).alias(b) for b, _, _ in valid_map])

for bronze_col, silver_col, _ in valid_map:
    if silver_col not in REQUIRED:
        continue
    bad = df_raw_sel.filter(F.col(bronze_col).isNull() | (F.col(bronze_col) == ""))
    error_frames.append(
        _error_rows(bad, silver_col, bronze_col, "null",
                    f"Required field '{silver_col}' is empty")
    )

# 2. Cast failures — non-empty raw value that could not be converted
for bronze_col, silver_col, dtype in valid_map:
    if dtype == "STRING":
        continue
    raw   = F.trim(F.col(bronze_col))
    casted = F.col(bronze_col).cast(dtype)
    bad = df_raw.filter(raw.isNotNull() & (raw != "") & casted.isNull())
    error_frames.append(
        _error_rows(bad, silver_col, bronze_col, "invalid_format",
                    f"Cannot parse '{silver_col}' as {dtype}")
    )

# 3. Latitude / longitude outside Canada bounding box
for silver_col, lo, hi, label in [
    ("latitude",  LAT_MIN, LAT_MAX, "Latitude"),
    ("longitude", LON_MIN, LON_MAX, "Longitude"),
]:
    if silver_col not in df_silver.columns:
        continue
    bad = df_silver.filter(
        F.col(silver_col).isNotNull() &
        ((F.col(silver_col) < lo) | (F.col(silver_col) > hi))
    )
    error_frames.append(
        _error_rows(bad, silver_col, silver_col, "invalid_range",
                    f"{label} outside the Canada bounding box ({lo} to {hi})")
    )

# 4. Invalid province code
if "province" in df_silver.columns:
    bad = df_silver.filter(
        F.col("province").isNotNull() &
        (F.trim(F.col("province")) != "") &
        ~F.upper(F.trim(F.col("province"))).isin(list(VALID_PROVINCES))
    )
    error_frames.append(
        _error_rows(bad, "province", "province", "invalid_value",
                    "Not a valid Canadian province / territory code")
    )

# Combine
if error_frames:
    df_errors = error_frames[0]
    for edf in error_frames[1:]:
        df_errors = df_errors.unionByName(edf)
else:
    df_errors = spark.createDataFrame([], ERROR_SCHEMA)

error_count = df_errors.count()
print(f"Errors detected : {error_count:,}")

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

# MAGIC %md ### Write field errors

# COMMAND ----------

# On first run the table won't exist — create it.
# On subsequent runs overwrite only this source table's rows so errors from
# other silver jobs (circuit_details, bh_traffic, etc.) are not affected.
errors_exist = spark.catalog.tableExists(f"{CATALOG}.{SILVER_SCHEMA}.field_errors")

if errors_exist:
    (
        df_errors.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_table = '{SOURCE_TABLE}'")
        .saveAsTable(ERRORS_REF)
    )
else:
    (
        df_errors.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ERRORS_REF)
    )

print(f"Written {error_count:,} error(s) to {ERRORS_REF}")

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
print(f"Run completed : {run_time}")
print(f"Silver rows   : {silver_count:,}")
print(f"Errors logged : {error_count:,}")

if error_count > 0:
    print("\nError breakdown:")
    (
        df_errors
        .groupBy("error_type", "field_name")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

if error_count > 0:
    raise Exception(
        f"{error_count} validation error(s) detected in '{SOURCE_TABLE}' — "
        f"see {ERRORS_REF} for details."
    )
