# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze → Silver: Column Selection
# MAGIC Selects curated columns from each bronze table, casts them to the correct types,
# MAGIC and writes the results to the matching silver tables.
# MAGIC
# MAGIC **To add a new table:** add an entry to `TABLE_CONFIG` below following the
# MAGIC same pattern as the existing entries.
# MAGIC
# MAGIC **Re-run safe:** each silver table is fully replaced on every run.

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

# ---------------------------------------------------------------------------
# TABLE_CONFIG
#
# Each entry defines one bronze → silver table transformation.
#
# Keys:
#   source      — bronze table name (also used as the silver table name)
#   columns     — list of (bronze_col, silver_col, target_type) tuples
#
#   bronze_col  — sanitised column name as it exists in the bronze table
#   silver_col  — name to use in the silver table (rename here if needed)
#   target_type — STRING keeps the value as-is
#                 INT / DOUBLE cast the value (nulls on failure are expected)
#
# To add a table, copy the placeholder block at the bottom and fill it in.
# ---------------------------------------------------------------------------

TABLE_CONFIG = [

    # ------------------------------------------------------------------
    # sites
    # ------------------------------------------------------------------
    {
        "source": "sites",
        "columns": [
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
        ],
    },
    {
        "source": "circuit_details",
        "columns": [
            ("site_a_site_code", "site_a_site_code", "STRING"),
            ("site_a_site_name", "site_a_site_name", "STRING"),
            ("site_a_province", "site_a_province", "STRING"),
            ("vendor", "vendor", "STRING"),
            ("term_after_circuit_expiry", "term_after_circuit_expiry", "STRING"),
            ("circuit_expiry", "circuit_expiry", "STRING"),
            ("site_b_site_name", "site_b_site_name", "STRING"),
            ("access_capacity_gbps", "access_capacity_gbps", "DOUBLE"),
            ("purchased_capacity_mbps", "purchased_capacity_mbps", "DOUBLE"),
            ("mrc", "mrc", "STRING"),
            ("site_a_latitude", "site_a_latitude", "DOUBLE"),
            ("site_a_longitude", "site_a_longitude", "DOUBLE"),
            ("circuit_status", "circuit_status", "STRING"),
            ("circuit_category", "circuit_category", "STRING"),
            ("circuit_user", "circuit_user", "STRING"),
            ("type_of_service", "type_of_service", "STRING"),   
        ],
    },
    {
        "source": "bh_traffic",
        "columns": [
            ("link_id", "link_id", "STRING"),
            ("link_name", "link_name", "STRING"),
            ("traffic_summary_95_ile", "traffic_summary_95_ile", "STRING"),
            ("traffic_summary_max", "traffic_summary_max", "STRING"),
            ("downlink_capacity_mbps", "downlink_capacity_mbps", "DOUBLE"),
            ("bh_links_designed_capacity", "bh_links_designed_capacity", "DOUBLE"),
            ("utilization_status_95_ile", "utilization_status_95_ile", "STRING"),
            ("utilization_status_max", "utilization_status_max", "STRING"),
            ("link_status", "link_status", "STRING"),
            ("province", "province", "STRING"),
            ("bh_links_network", "bh_links_network", "STRING"),
            ("utilization_95_ile", "utilization_95_ile", "STRING"),
            ("utilization_max", "utilization_max", "STRING"),
            ("congestionstartdate", "congestion_start_date", "STRING"),
            ("linkcode", "linkcode", "STRING"),
            ("uplink_and_inbound_utilization_past_week_95", "uplink_and_inbound_utilization_past_week_95", "STRING"),
            ("uplink_and_inbound_utilization_past_week_max", "uplink_and_inbound_utilization_past_week_max", "STRING"),
            ("uplink_and_outbound_utilization_past_week_95", "uplink_and_outbound_utilization_past_week_95", "STRING"),
            ("uplink_and_outbound_utilization_past_week_max", "uplink_and_outbound_utilization_past_week_max", "STRING"),
        ],
    },
    {
        "source": "circuit_traffic",
        "columns": [
           ("circuit_status", "circuit_status", "STRING"),
           ("circuit_site_a_province", "circuit_site_a_province", "STRING"),
           ("circuit_site_a_code", "circuit_site_a_code", "STRING"),
           ("circuit_name_location", "circuit_name_location", "STRING"),
           ("network_name", "network_name", "STRING"),
           ("vendor", "vendor", "STRING"),
           ("circuit_capacity_mbps", "circuit_capacity_mbps", "DOUBLE"),
           ("past_week_max_traffic_mbps", "past_week_max_traffic_mbps", "STRING"),
           ("utilization_max_past_week", "utilization_max_past_week", "STRING"),
           ("utilization_status_max_past_week", "utilization_status_max_past_week", "STRING"),
           ("past_week_95th_traffic_mbps", "past_week_95th_traffic_mbps", "STRING"),
           ("utilization_95th_past_week", "utilization_95th_past_week", "STRING"),
           ("utilization_status_95_ile", "utilization_status_95_ile", "STRING"),
           ("packet_loss_last_week", "packet_loss_last_week", "STRING"),
        ],
    },
    {

        "source": "peer_and_internal_traffic",
        "columns": [
            ("site_a_network_name", "site_a_network_name", "STRING"),
            ("site_a_code", "site_a_code", "STRING"),
            ("site_a_name", "site_a_name", "STRING"),
            ("site_b_name", "site_b_name", "STRING"),
            ("circuit_category", "circuit_category", "STRING"),
            ("interface_status", "interface_status", "STRING"),
            ("interface_name_location", "interface_name_location", "STRING"),
            ("capacity_mbps", "capacity_mbps", "DOUBLE"),
            ("utilization_status_max_traffic", "utilization_status_max_traffic", "STRING"),
            ("past_week_max_traffic_mbps", "past_week_max_traffic_mbps", "STRING"),
            ("max_utilization_past_week", "max_utilization_past_week", "STRING"),
            ("utilization_status_95_ile", "utilization_status_95_ile", "STRING"),
            ("past_week_95th_traffic_mbps", "past_week_95th_traffic_mbps", "STRING"),
            ("utilization_95th_past_week", "utilization_95th_past_week", "STRING"),
        ],

    }, 
    {
        "source": "indexed_site_lookup",
        "columns": [
            ("site_id", "site_id", "INT"),
            ("site_code", "site_code", "STRING"),
            ("site_name", "site_name", "STRING"),
            ("site_status", "site_status", "STRING"),
            ("site_index", "site_index", "INT"),
            ("site_id_top", "site_id_top", "INT"),
            ("site_code_top", "site_code_top", "STRING"),
            ("pop_site_id", "pop_site_id", "INT"),
            ("pop_site_code", "pop_site_code", "STRING"),
            ("cx_count", "cx_count", "DOUBLE"),
            ("max_capacity", "max_capacity", "DOUBLE"),
            ("ltp_status", "ltp_status", "STRING"),
        ],
    }, 
    {
        "source": "path_to_pop",
        "columns": [
            ("site_id", "site_id", "INT"),
            ("site_code", "site_code", "STRING"),
            ("path_to_pop", "path_to_pop", "STRING"),
        ],
    }, 
    {
        "source": "xi_bh_connections_rs",
        "columns": [
            ("upstreamsite_site_id", "upstream_site_id", "INT"),
            ("upstreamsite_site_code", "upstream_site_code", "STRING"),
            ("upstreamsite_longitude", "upstream_site_longitude", "DOUBLE"),
            ("upstreamsite_latitude", "upstream_site_latitude", "DOUBLE"),
            ("downstreamsite_site_id", "downstream_site_id", "INT"),
            ("downstreamsite_site_code", "downstream_site_code", "STRING"),
            ("downstreamsite_longitude", "downstream_site_longitude", "DOUBLE"),
            ("downstreamsite_latitude", "downstream_site_latitude", "DOUBLE"),
            ("link_id", "link_id", "STRING"),
            ("link_name", "link_name", "STRING"),
            ("province", "province", "STRING"),
            ("link_status", "link_status", "STRING"),
            ("primarybh_live", "primary_bh_live", "STRING"),
            ("max_capacity", "max_capacity", "DOUBLE"),
            ("designed_radio", "designed_radio", "STRING"),
            ("designed_capacity", "designed_capacity", "DOUBLE"),
            ("traffic_summary_95_ile", "traffic_summary_95_ile", "STRING"),
            ("traffic_summary_max", "traffic_summary_max", "STRING"),
            ("link_length", "link_length", "DOUBLE"),
            ("project", "project", "INT"), 

        ],
    }, 
    {
        "source": "xi_bh_connections_analysis",
        "columns": [
            ("upstreamsite_site_code", "upstream_site_code", "STRING"),
            ("downstreamsite_site_code", "downstream_site_code", "STRING"),
             ("link_id", "link_id", "STRING"),
            ("link_name", "link_name", "STRING"),
            ("province", "province", "STRING"),
            ("link_status", "link_status", "STRING"),
            ("primarybh_live", "primary_bh_live", "STRING"),
            ("max_capacity", "max_capacity", "DOUBLE"),
            ("designed_radio", "designed_radio", "STRING"),
            ("designed_capacity", "designed_capacity", "DOUBLE"),
            ("traffic_summary_95_ile", "traffic_summary_95_ile", "STRING"),
            ("traffic_summary_max", "traffic_summary_max", "STRING"),
            ("link_length", "link_length", "DOUBLE"),
            ("project", "project", "INT"), 

        ],
    }, 

    # ------------------------------------------------------------------
    # Add new tables here following the same pattern:
    # ------------------------------------------------------------------
    # {
    #     "source": "table_name",
    #     "columns": [
    #         ("bronze_col", "silver_col", "TYPE"),
    #     ],
    # },

]

# COMMAND ----------

# MAGIC %md ### Ingestion loop

# COMMAND ----------

def process_table(cfg: dict) -> int:
    source      = cfg["source"]
    column_map  = cfg["columns"]
    bronze_ref  = f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{source}`"
    silver_ref  = f"`{CATALOG}`.`{SILVER_SCHEMA}`.`{source}`"

    print(f"Processing : {source}")

    df_raw      = spark.table(bronze_ref)
    bronze_cols = set(df_raw.columns)

    missing   = [b for b, _, _ in column_map if b not in bronze_cols]
    valid_map = [(b, s, t) for b, s, t in column_map if b in bronze_cols]

    if missing:
        print(f"  WARNING — columns not found in bronze (skipped): {missing}")

    print(f"  Columns : {len(valid_map)} of {len(column_map)} mapped")

    select_exprs = []
    for bronze_col, silver_col, dtype in valid_map:
        col = F.trim(F.col(bronze_col))
        if dtype != "STRING":
            col = col.cast(dtype)
        select_exprs.append(col.alias(silver_col))

    df_silver = df_raw.select(select_exprs)

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_ref)
    )

    row_count = spark.table(silver_ref).count()
    print(f"  Written  : {row_count:,} rows → {silver_ref}\n")
    return row_count


# COMMAND ----------

results = []

for cfg in TABLE_CONFIG:
    try:
        count = process_table(cfg)
        results.append({"table": cfg["source"], "rows": count, "status": "ok"})
    except Exception as e:
        print(f"  ERROR : {e}\n")
        results.append({"table": cfg["source"], "rows": 0, "status": str(e)})

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
n_ok     = sum(1 for r in results if r["status"] == "ok")
n_err    = sum(1 for r in results if r["status"] != "ok")

print(f"Run completed : {run_time}")
print(f"Success : {n_ok}   Errors : {n_err}\n")

for r in results:
    icon = "✓" if r["status"] == "ok" else "✗"
    print(f"  {icon}  {r['table']:<35}  {r['rows']:>8,} rows   {r['status']}")

if n_err:
    raise Exception(f"{n_err} table(s) failed — see output above for details.")
