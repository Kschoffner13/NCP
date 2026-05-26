"""
Databricks SQL connection helper.

Provides:
  get_cursor() — context manager that opens and closes a connection per call
  table()      — builds a fully-qualified Delta table name from a schema key + table name
  CATALOG      — active catalog name (from env)
  SCHEMAS      — mapping of tier names to schema names (from env)
"""

import os
from contextlib import contextmanager

from databricks import sql

CATALOG = os.getenv("DATABRICKS_CATALOG", "network_capacity_planning")

SCHEMAS = {
    "raw":    os.getenv("DATABRICKS_SCHEMA_RAW",    "00_landing"),
    "bronze": os.getenv("DATABRICKS_SCHEMA_BRONZE", "01_bronze"),
    "silver": os.getenv("DATABRICKS_SCHEMA_SILVER", "02_silver"),
    "gold":   os.getenv("DATABRICKS_SCHEMA_GOLD",   "03_gold"),
}


def table(schema: str, name: str) -> str:
    """Return a fully-qualified backtick-quoted table reference.

    Example:
        table("silver", "sites")
        → `network_capacity_planning`.`02_silver`.`sites`
    """
    return f"`{CATALOG}`.`{SCHEMAS[schema]}`.`{name}`"


@contextmanager
def get_cursor():
    conn = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()
