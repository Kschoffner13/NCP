"""
Creates the field_errors and cross_table_errors tables in 02_silver.
Safe to re-run — uses CREATE TABLE IF NOT EXISTS.

Run from backend/:
    python create_error_tables.py
"""

from dotenv import load_dotenv
load_dotenv()

from db import get_cursor, CATALOG, SCHEMAS

SILVER = SCHEMAS["silver"]

TABLES = {
    "field_errors": f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SILVER}`.`field_errors` (
            source_table  STRING  NOT NULL  COMMENT 'Table the error came from (e.g. sites, circuit_details)',
            record_id     STRING            COMMENT 'Primary key value of the offending row',
            field_name    STRING            COMMENT 'Column that failed validation',
            field_value   STRING            COMMENT 'Actual value that caused the error',
            error_type    STRING            COMMENT 'null | invalid_format | invalid_range | invalid_value',
            error_message STRING            COMMENT 'Human-readable description of the problem',
            detected_at   TIMESTAMP         COMMENT 'When the validation job ran'
        )
        USING DELTA
        PARTITIONED BY (source_table)
        COMMENT 'Single-field validation errors produced by silver pipeline jobs'
    """,

    "cross_table_errors": f"""
        CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SILVER}`.`cross_table_errors` (
            error_type        STRING  NOT NULL  COMMENT 'orphan_reference | value_mismatch | missing_record',
            source_table      STRING  NOT NULL  COMMENT 'Table where the discrepancy was found',
            source_record_id  STRING            COMMENT 'Primary key of the row in source_table',
            source_field      STRING            COMMENT 'Field in source_table that references another table',
            source_value      STRING            COMMENT 'Value that could not be matched',
            reference_table   STRING            COMMENT 'Table being referenced or compared against',
            reference_field   STRING            COMMENT 'Field in reference_table used for the lookup',
            reference_value   STRING            COMMENT 'Value found in reference_table (null if missing)',
            error_message     STRING            COMMENT 'Human-readable description of the discrepancy',
            detected_at       TIMESTAMP         COMMENT 'When the validation job ran'
        )
        USING DELTA
        PARTITIONED BY (source_table)
        COMMENT 'Cross-table discrepancies produced by cross-validation pipeline jobs'
    """,
}


def main():
    print(f"Catalog : {CATALOG}")
    print(f"Schema  : {SILVER}\n")

    with get_cursor() as cursor:
        for table_name, ddl in TABLES.items():
            cursor.execute(ddl)
            print(f"Ready: `{CATALOG}`.`{SILVER}`.`{table_name}`")

    print("\nDone.")


if __name__ == "__main__":
    main()
