import os
from datetime import datetime, timezone

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "ref_files")

VALID_PROVINCES = {
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
}

# Canada bounding box
LAT_MIN, LAT_MAX = 41.7, 83.1
LON_MIN, LON_MAX = -141.0, -52.6

TABLE_CONFIG = [
    {
        "source": "sites",
        "file": "Sites.csv",
        "id_col": "Site ID#",
        "required": ["Site ID#", "Site Code", "Site Name", "Site Status"],
        "numeric": ["Latitude", "Longitude"],
        "lat_col": "Latitude",
        "lon_col": "Longitude",
        "province_col": "Province",
    },
    {
        "source": "circuit_details",
        "file": "Circuit_details.csv",
        "id_col": "Record ID#",
        "required": ["Record ID#", "Circuit ID", "Site A - Site Code", "Circuit Status"],
        "numeric": ["Access Capacity (Gbps)", "Site A - Latitude", "Site A - Longitude"],
        "lat_col": "Site A - Latitude",
        "lon_col": "Site A - Longitude",
        "province_col": "Site A - Province",
    },
    {
        "source": "bh_traffic",
        "file": "BH_Traffic.csv",
        "id_col": "Link ID",
        "required": ["Link ID", "Link Name", "Link Status"],
        "numeric": ["Downlink Capacity (Mbps)", "Utilization (95%ile)", "Utilization (Max)"],
        "lat_col": None,
        "lon_col": None,
        "province_col": "Province",
    },
    {
        "source": "circuit_traffic",
        "file": "Circuit_Traffic.csv",
        "id_col": "Record ID#",
        "required": ["Record ID#", "Circuit ID", "Circuit Status"],
        "numeric": ["Circuit Capacity (Mbps)", "Past Week Max Traffic (Mbps)", "Past Week 95th Traffic (Mbps)"],
        "lat_col": None,
        "lon_col": None,
        "province_col": "Circuit - Site A - Province",
    },
]


def _err(source, record_id, field_name, field_value, error_type, message):
    return {
        "source": source,
        "record_id": str(record_id),
        "field_name": field_name,
        "field_value": str(field_value),
        "error_type": error_type,
        "error_message": message,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def _to_float(raw: str) -> float:
    return float(raw.replace(",", "").replace("%", "").strip())


def _check_nulls(df, source, id_col, required_cols):
    errors = []
    for col in required_cols:
        if col not in df.columns:
            continue
        mask = df[col].str.strip() == ""
        for _, row in df[mask].iterrows():
            errors.append(_err(source, row[id_col], col, "", "null", f"Required field '{col}' is empty"))
    return errors


def _check_numeric(df, source, id_col, numeric_cols):
    errors = []
    for col in numeric_cols:
        if col not in df.columns:
            continue
        non_empty = df[df[col].str.strip() != ""]
        for _, row in non_empty.iterrows():
            try:
                _to_float(row[col])
            except (ValueError, TypeError):
                errors.append(_err(
                    source, row[id_col], col, row[col],
                    "invalid_format", f"Expected a numeric value, got '{row[col]}'"
                ))
    return errors


def _check_lat_lon(df, source, id_col, lat_col, lon_col):
    errors = []
    checks = [
        (lat_col, LAT_MIN, LAT_MAX, "Latitude"),
        (lon_col, LON_MIN, LON_MAX, "Longitude"),
    ]
    for col, lo, hi, label in checks:
        if not col or col not in df.columns:
            continue
        non_empty = df[df[col].str.strip() != ""]
        for _, row in non_empty.iterrows():
            try:
                val = _to_float(row[col])
                if not (lo <= val <= hi):
                    errors.append(_err(
                        source, row[id_col], col, row[col],
                        "invalid_range",
                        f"{label} {val} is outside the valid Canada range ({lo} to {hi})"
                    ))
            except (ValueError, TypeError):
                pass  # already caught by _check_numeric
    return errors


def _check_province(df, source, id_col, province_col):
    errors = []
    if not province_col or province_col not in df.columns:
        return errors
    non_empty = df[df[province_col].str.strip() != ""]
    for _, row in non_empty.iterrows():
        val = row[province_col].strip().upper()
        if val not in VALID_PROVINCES:
            errors.append(_err(
                source, row[id_col], province_col, row[province_col],
                "invalid_value",
                f"'{row[province_col]}' is not a valid Canadian province/territory code"
            ))
    return errors


def run_validation() -> list[dict]:
    all_errors = []
    for cfg in TABLE_CONFIG:
        filepath = os.path.join(DATA_DIR, cfg["file"])
        if not os.path.exists(filepath):
            continue
        for enc in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                df = pd.read_csv(filepath, dtype=str, keep_default_na=False, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            continue  # skip file if all encodings fail
        src = cfg["source"]
        id_col = cfg["id_col"]
        if id_col not in df.columns:
            continue

        all_errors += _check_nulls(df, src, id_col, cfg["required"])
        all_errors += _check_numeric(df, src, id_col, cfg["numeric"])
        all_errors += _check_lat_lon(df, src, id_col, cfg.get("lat_col"), cfg.get("lon_col"))
        all_errors += _check_province(df, src, id_col, cfg.get("province_col"))

    return all_errors
