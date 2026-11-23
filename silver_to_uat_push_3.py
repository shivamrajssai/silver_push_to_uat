"""Push Silver Parquet tables from S3 into PostgreSQL DB (dhms_staging / public schema).

Tables handled:
  1.  geometry_parent.parquet            -> geometry_parent
  2.  geometry_child.parquet             -> geometry_child
  3.  master_roughness_parent.parquet    -> master_roughness_parent
  4.  master_roughness_child_10m.parquet -> master_roughness_child_10m
  5.  master_roughness_as_per_ca.parquet -> master_roughness_as_per_ca
  6.  master_rutting_parent.parquet      -> master_rutting_parent
  7.  master_rutting_child_10m.parquet   -> master_rutting_child_10m
  8.  master_rutting_as_per_ca.parquet   -> master_rutting_as_per_ca
  9.  master_cracking_10m.parquet        -> master_cracking_10m (fallback: master_cracking_child_10m)
  10. master_cracking_as_per_ca.parquet  -> master_cracking_as_per_ca
  11. master_ravelling.parquet           -> master_ravelling
  12. master_ravelling_10m.parquet       -> master_ravelling_10m
  13. master_pothole.parquet             -> master_pothole
  14. master_chainage.parquet            -> master_chainage (reports MISSING if table not in DB)

Key features:
  - Input parsing: takes UPC, cycle, and phase (e.g. 'N/01002/01001/GJ 1 1')
  - Auto-discovers Zone from master_upc_project or searches S3 prefixes across candidate zones (B, A, C, D, E).
  - Matches S3 Parquet schema with PostgreSQL table schema (only common writable columns pushed).
  - Rows matched on `id`:
      id already in DB -> UPDATE every pushed column except id and created_at
      id not in DB     -> INSERT full row
  - Converts Parquet values to target PostgreSQL types in the pipeline (including text[], UUID,
    integer/bigint, timestamps, dates, numeric, booleans, and JSON).
  - Does not alter DB schema and does not silently nullify invalid foreign keys.
  - Handles defaults on NOT NULL columns (e.g. count columns defaulting to 0).
  - Batch-aware Foreign Key checking: validates FKs against both DB and current batch parents.
  - "As per IRC" rule: in every *_10m parquet, rows whose text contains "As per IRC" are NOT pushed
    (neither inserted nor updated). Skipped rows are exported to dry_run_output/<table>_skipped_irc.csv.
    Optional env IRC_FILTER_COLUMNS=col1,col2 restricts the scan to specific columns.
  - Constraint-driven value conversion: master_ravelling.level 'master' -> 'parent' so that
    master_ravelling_level_check (level = 'parent') passes. Extend VALUE_CONVERSIONS for others.
  - Audit log: dry_run_output/silver_push_audit_<timestamp>.csv records skipped rows, conversions,
    validation failures, and live-run insert/update outcomes.
  - DRY_RUN (default True): validates types and foreign keys, previews changes to dry_run_output/.
  - LIVE RUN (--dry-run false): writes all tables in ONE atomic transaction.

Usage:
    # 1. Using CLI arguments:
    python db_push/push_silver_to_db.py --upc "N/01002/01001/GJ" --cycle 1 --phase 1

    # 2. Live push:
    python db_push/push_silver_to_db.py --upc "N/01002/01001/GJ" --cycle 1 --phase 1 --dry-run false

    # 3. Pushing from a local directory instead of S3 (for testing):
    python db_push/push_silver_to_db.py --local-dir /path/to/dlm_kpi_new --dry-run true

    # 4. Supplying input line or file (upc cycle phase format):
    python db_push/push_silver_to_db.py --input "N/01002/01001/GJ 1 1"
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
import botocore.exceptions
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------------------------------------------------
DEFAULT_DRY_RUN = True
DEFAULT_S3_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver"
PAGE_SIZE = 1000
SAMPLE_ROWS = 5
KEY = "id"
CREATED_AT = "created_at"
UPDATED_AT = "updated_at"
NOT_UPDATED = {KEY, CREATED_AT}
INT_TYPES = {"smallint", "integer", "bigint"}

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / ".env"
PREVIEW_DIR = HERE / "dry_run_output"

# Rows containing this text in any scanned column of a *_10m parquet are never pushed.
IRC_PATTERN = re.compile(r"as[\s_\-]*per[\s_\-]*irc", re.IGNORECASE)

# Constraint-driven value conversions applied before validation/write.
# {target_table: {column: {"map": {source_value_lower: db_value}, "allowed": {db values accepted by CHECK}}}}
# master_ravelling_level_check = CHECK (level::text = 'parent'::text)
VALUE_CONVERSIONS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "master_ravelling": {
        "level": {"map": {"master": "parent"}, "allowed": {"parent"}},
    },
}

ROW_LEVEL_CONSTRAINT_TABLES = {
    "master_ravelling",
    "master_ravelling_10m",
}

# 14 Silver tables in topological dependency order (parents before children)
# Format: (parquet_filename, primary_table_name, [fallback_table_names])
SILVER_TABLE_SPECS = [
    ("geometry_parent.parquet", "geometry_parent", []),
    ("geometry_child.parquet", "geometry_child", []),
    ("master_roughness_parent.parquet", "master_roughness_parent", []),
    ("master_roughness_child_10m.parquet", "master_roughness_child_10m", []),
    ("master_roughness_as_per_ca.parquet", "master_roughness_as_per_ca", []),
    ("master_rutting_parent.parquet", "master_rutting_parent", []),
    ("master_rutting_child_10m.parquet", "master_rutting_child_10m", []),
    ("master_rutting_as_per_ca.parquet", "master_rutting_as_per_ca", []),
    ("master_cracking_10m.parquet", "master_cracking_child_10m", []),
    ("master_cracking_as_per_ca.parquet", "master_cracking_as_per_ca", []),
    ("master_ravelling.parquet", "master_ravelling", []),
    ("master_ravelling_10m.parquet", "master_ravelling_10m", []),
    ("master_pothole.parquet", "master_pothole", []),
    ("master_chainage.parquet", None, []),  # Schema checker confirms no target DB table
]

log = logging.getLogger("push_silver_to_db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# In-memory audit trail, flushed to CSV at the end of the run.
AUDIT: List[Dict[str, Any]] = []


def audit_event(table: str, event: str, rows: int = 0, detail: str = "") -> None:
    AUDIT.append(
        {
            "logged_at": datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds"),
            "table": table,
            "event": event,
            "rows": rows,
            "detail": detail,
        }
    )


def write_audit_log(run_time: str, dry_run: bool) -> None:
    if not AUDIT:
        return
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = re.sub(r"[^0-9]", "", run_time)[:14]
    path = PREVIEW_DIR / f"silver_push_audit_{stamp}.csv"
    df = pd.DataFrame(AUDIT)
    df.insert(1, "mode", "DRY_RUN" if dry_run else "LIVE")
    df.to_csv(path, index=False)
    print(f"Audit log written: {path}")


# ---------------------------------------------------------------------------------------------------------------------
# Environment & DB Settings
# ---------------------------------------------------------------------------------------------------------------------
def load_env(path: Path = ENV_FILE) -> None:
    """Load KEY=VALUE lines into os.environ if not already set."""
    if not path.exists():
        root_env = path.parent.parent / ".env"
        if root_env.exists():
            path = root_env
        else:
            return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def db_settings() -> Dict[str, Any]:
    missing = [k for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing DB settings {missing}: set them in {ENV_FILE} or as environment variables")
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


def get_s3_client():
    """Create S3 client using environment variables or AWS default credential chain."""
    kwargs = {}
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        kwargs["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
        if os.environ.get("AWS_SESSION_TOKEN"):
            kwargs["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]
    if os.environ.get("AWS_REGION"):
        kwargs["region_name"] = os.environ["AWS_REGION"]
    elif os.environ.get("AWS_DEFAULT_REGION"):
        kwargs["region_name"] = os.environ["AWS_DEFAULT_REGION"]
    else:
        kwargs["region_name"] = "ap-south-1"
    return boto3.client("s3", **kwargs)


# ---------------------------------------------------------------------------------------------------------------------
# UPC & Zone Helpers
# ---------------------------------------------------------------------------------------------------------------------
def parse_upc_variations(upc_raw: str) -> List[str]:
    """Generate various representations of a UPC (slashes, underscores, hyphens)."""
    clean = str(upc_raw).strip()
    slash = clean.replace("_", "/").replace("-", "/")
    parts = [p for p in slash.split("/") if p]
    if len(parts) == 4:
        return [
            "/".join(parts),               # N/01002/01001/GJ
            "_".join(parts),               # N_01002_01001_GJ
            "-".join(parts),               # N-01002-01001-GJ
        ]
    return [clean, clean.replace("/", "_"), clean.replace("/", "-")]


def resolve_zone_from_db(cur, upc_raw: str) -> Optional[str]:
    """Look up Zone from master_upc_project & master_zone if not provided."""
    variations = parse_upc_variations(upc_raw)
    try:
        cur.execute(
            """
            SELECT mz.zone_code
            FROM master_upc_project p
            JOIN master_zone mz ON p.nsv_zone_id = mz.id
            WHERE p.upc = ANY(%s)
            LIMIT 1;
            """,
            (variations,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    except Exception as e:
        log.warning(f"Could not resolve zone from DB for UPC {upc_raw}: {e}")
        cur.connection.rollback()
    return None


# ---------------------------------------------------------------------------------------------------------------------
# S3 File Discovery
# ---------------------------------------------------------------------------------------------------------------------
def locate_s3_parquet_files(
    s3_client,
    s3_root: str,
    upc: str,
    phase: str,
    cycle: str,
    zone: Optional[str] = None,
) -> Dict[str, str]:
    """
    Search S3 for the 14 silver parquet files matching the UPC, Phase, Cycle, and Zone.
    Returns {parquet_filename: s3_uri}.
    """
    if not s3_root.startswith("s3://"):
        raise ValueError(f"s3_root must start with s3://: {s3_root}")

    root_parts = s3_root[5:].split("/", 1)
    bucket = root_parts[0]
    base_prefix = root_parts[1].strip("/") if len(root_parts) > 1 else ""

    upc_vars = parse_upc_variations(upc)
    candidate_zones = [zone] if zone else ["B", "A", "C", "D", "E"]

    expected_files = {spec[0] for spec in SILVER_TABLE_SPECS}
    found_files: Dict[str, str] = {}

    log.info(f"Locating Parquet files in s3://{bucket}/{base_prefix}/ for UPC={upc}, Phase={phase}, Cycle={cycle}...")

    for z in candidate_zones:
        if len(found_files) == len(expected_files):
            break
        for u in upc_vars:
            if len(found_files) == len(expected_files):
                break
            possible_prefixes = [
                f"{base_prefix}/Zone={z}/Phase={phase}/Cycle={cycle}/UPC={u}/dlm_kpi_new",
                f"{base_prefix}/Zone={z}/Phase={phase}/Cycle={cycle}/UPC={u}",
                f"{base_prefix}/zone={z}/phase={phase}/cycle={cycle}/{u}/dlm_kpi_new",
                f"{base_prefix}/zone={z}/phase={phase}/cycle={cycle}/{u}",
                f"nsv-silver/zone={z}/phase={phase}/cycle={cycle}/{u}/dlm_kpi_new",
                f"nsv-silver/zone={z}/phase={phase}/cycle={cycle}/{u}",
            ]
            for prefix in possible_prefixes:
                prefix_clean = prefix.strip("/")
                try:
                    paginator = s3_client.get_paginator("list_objects_v2")
                    prefix_found = False
                    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_clean):
                        for obj in page.get("Contents", []):
                            key = obj["Key"]
                            fname = key.split("/")[-1]
                            if fname in expected_files and fname not in found_files:
                                found_files[fname] = f"s3://{bucket}/{key}"
                                prefix_found = True
                    if prefix_found:
                        log.info(f"  Found matching partition at s3://{bucket}/{prefix_clean}/ (Zone={z})")
                        break
                except botocore.exceptions.ClientError as e:
                    log.debug(f"S3 list error on prefix {prefix_clean}: {e}")

    return found_files


def locate_local_parquet_files(local_dir: Path) -> Dict[str, str]:
    """Scan a local directory (or its subdirectories) for the 14 silver parquet files."""
    found: Dict[str, str] = {}
    expected = {spec[0] for spec in SILVER_TABLE_SPECS}
    local_dir = Path(local_dir)
    if not local_dir.is_dir():
        log.warning(f"Local path {local_dir} is not a valid directory.")
        return found

    for root, _, files in os.walk(local_dir):
        for f in files:
            if f in expected and f not in found:
                found[f] = str(Path(root) / f)
    return found


def read_parquet_source(source: str, s3_client=None) -> pd.DataFrame:
    """Read a Parquet file from an S3 URI or a local path into a DataFrame."""
    if str(source).startswith("s3://"):
        bucket, key = str(source)[5:].split("/", 1)
        client = s3_client or get_s3_client()
        obj = client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    else:
        return pd.read_parquet(source)


# ---------------------------------------------------------------------------------------------------------------------
# Database Metadata & Schema Inspection
# ---------------------------------------------------------------------------------------------------------------------
def db_table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s;
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def db_columns(cur, schema: str, table: str) -> Dict[str, Dict[str, Any]]:
    """Retrieve column metadata including default expressions."""
    cur.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atthasdef,
               pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
               a.attgenerated = '' AND a.attidentity <> 'a'
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;
        """,
        (schema, table),
    )
    return {
        name: {
            "type": typ,
            "not_null": not_null,
            "has_default": has_def,
            "default_expr": default_expr,
            "writable": writable,
        }
        for name, typ, not_null, has_def, default_expr, writable in cur.fetchall()
    }


def existing_ids(cur, schema: str, table: str, key_col: str = KEY) -> Set[str]:
    cur.execute(
        sql.SQL("SELECT {}::text FROM {}.{}").format(
            sql.Identifier(key_col), sql.Identifier(schema), sql.Identifier(table)
        )
    )
    return {str(row[0]).strip() for row in cur.fetchall() if row[0] is not None}


def foreign_keys(cur, schema: str, table: str) -> List[Tuple[str, str, str, str]]:
    cur.execute(
        """
        SELECT a.attname, rn.nspname, rc.relname, ra.attname
        FROM pg_constraint k
        JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class rc ON rc.oid = k.confrelid JOIN pg_namespace rn ON rn.oid = rc.relnamespace
        JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
        JOIN pg_attribute ra ON ra.attrelid = k.confrelid AND ra.attnum = k.confkey[1]
        WHERE k.contype = 'f' AND n.nspname = %s AND c.relname = %s AND cardinality(k.conkey) = 1;
        """,
        (schema, table),
    )
    return cur.fetchall()


# ---------------------------------------------------------------------------------------------------------------------
# Value Cleaning & Normalization
# ---------------------------------------------------------------------------------------------------------------------
def is_null_value(val: Any) -> bool:
    """Scalar-safe null check (pd.isna(list/array) returns an array)."""
    if val is None:
        return True
    if isinstance(val, str):
        return val.strip().lower() in ("", "nan", "none", "nat", "null")
    try:
        result = pd.isna(val)
        return bool(result)
    except (TypeError, ValueError):
        # Arrays/lists produce array-valued null masks, not scalar nulls.
        return False


def format_pg_array(val: Any) -> Optional[List[Any]]:
    """
    Convert Parquet scalar/list/JSON-array/PG-array/comma-separated values
    to a Python list. psycopg2 adapts Python lists as PostgreSQL arrays.
    """
    if is_null_value(val):
        return None

    if isinstance(val, (list, tuple, set)):
        return [x.item() if hasattr(x, "item") else x for x in val if not is_null_value(x)]

    if hasattr(val, "tolist") and not isinstance(val, (str, bytes, bytearray)):
        converted = val.tolist()
        if isinstance(converted, list):
            return format_pg_array(converted)

    text_value = str(val).strip()
    if not text_value or text_value.lower() in ("none", "nan", "null"):
        return None

    # JSON array or Python-list representation stored in a Parquet string.
    if text_value.startswith("[") and text_value.endswith("]"):
        import ast
        import json
        try:
            parsed = json.loads(text_value)
        except Exception:
            try:
                parsed = ast.literal_eval(text_value)
            except Exception:
                parsed = None
        if isinstance(parsed, (list, tuple)):
            return [x for x in parsed if not is_null_value(x)]

    # PostgreSQL array literal already stored in source.
    if text_value.startswith("{") and text_value.endswith("}"):
        inner = text_value[1:-1]
        if not inner:
            return []
        # Basic parser for common text[] values; quoted commas are preserved.
        import csv
        try:
            return next(csv.reader([inner], delimiter=",", quotechar='"', escapechar="\\"))
        except Exception:
            return [inner]

    # Treat a plain scalar as one array element; split conventional CSV values.
    if "," in text_value:
        return [part.strip() for part in text_value.split(",") if part.strip()]
    return [text_value]


def clean_cell_value(val: Any, col_type: str) -> Any:
    """Convert a Parquet cell to a psycopg2-adaptable value for the target DB type."""
    if is_null_value(val):
        return None

    normalized_type = col_type.lower()

    # PostgreSQL arrays (text[], uuid[], integer[], etc.).
    if normalized_type.endswith("[]"):
        return format_pg_array(val)

    # JSON / JSONB: pass serialized JSON text for PostgreSQL casting.
    if normalized_type.startswith("json"):
        import json
        if isinstance(val, (dict, list, tuple)):
            return json.dumps(val, default=str)
        return str(val)

    # Preserve binary values for bytea columns.
    if normalized_type == "bytea" and isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val)

    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime().isoformat()
    if isinstance(val, datetime):
        return val.isoformat()

    # NumPy scalar -> native Python scalar.
    if hasattr(val, "item") and not isinstance(val, (str, bytes, bytearray)):
        try:
            val = val.item()
        except Exception:
            pass

    text_value = str(val).strip()
    if not text_value or text_value.lower() in ("nan", "none", "nat", "null"):
        return None

    # Integer columns: reject non-integral values instead of silently truncating.
    if normalized_type in INT_TYPES:
        try:
            number = float(text_value)
            if not number.is_integer():
                raise ValueError(f"Non-integral value {text_value!r} for {col_type}")
            return str(int(number))
        except ValueError:
            raise ValueError(f"Cannot convert {text_value!r} to {col_type}")

    if normalized_type == "uuid":
        try:
            return str(uuid.UUID(text_value))
        except ValueError as exc:
            raise ValueError(f"Invalid UUID value {text_value!r}") from exc

    if normalized_type == "boolean":
        lowered = text_value.lower()
        if lowered in ("true", "t", "1", "yes", "y"):
            return True
        if lowered in ("false", "f", "0", "no", "n"):
            return False
        raise ValueError(f"Cannot convert {text_value!r} to boolean")

    # Numeric-to-varchar and all other scalar-to-text conversions.
    return text_value


def rows_of(df: pd.DataFrame, cols: List[str], col_types: Dict[str, str]) -> List[Tuple]:
    return [
        tuple(clean_cell_value(v, col_types[c]) for c, v in zip(cols, row))
        for row in df[cols].itertuples(index=False, name=None)
    ]


def chunks(items: List[Any], size: int = PAGE_SIZE):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def values_template(cols: List[str], columns_meta: Dict[str, Dict[str, Any]]) -> str:
    """(%s::type, ...) so every text value is explicitly cast to its DB column type."""
    return "(" + ", ".join(f"%s::{columns_meta[c]['type']}" for c in cols) + ")"


def ident_list(cols: List[str]) -> sql.Composable:
    return sql.SQL(", ").join(sql.Identifier(c) for c in cols)


# ---------------------------------------------------------------------------------------------------------------------
# Business Rules: "As per IRC" exclusion (*_10m) & constraint-driven value conversion
# ---------------------------------------------------------------------------------------------------------------------
def is_10m_file(parquet_filename: str) -> bool:
    return parquet_filename.lower().endswith("_10m.parquet")


def exclude_as_per_irc_rows(
    df: pd.DataFrame, parquet_filename: str, table: str
) -> pd.DataFrame:
    """
    For *_10m parquets only: drop every row where any scanned text column contains 'As per IRC'.
    Scans all text columns, or only those listed in env IRC_FILTER_COLUMNS (comma-separated).
    Dropped rows are exported to dry_run_output/<table>_skipped_irc.csv and audit-logged.
    """
    if not is_10m_file(parquet_filename) or df.empty:
        return df

    override = [c.strip() for c in os.environ.get("IRC_FILTER_COLUMNS", "").split(",") if c.strip()]
    if override:
        scan_cols = [c for c in override if c in df.columns]
        missing = [c for c in override if c not in df.columns]
        if missing:
            print(f"  Note: IRC_FILTER_COLUMNS not in {parquet_filename}: {missing}")
    else:
        scan_cols = [c for c in df.columns if df[c].dtype == object or str(df[c].dtype).startswith("string")]

    mask = pd.Series(False, index=df.index)
    hit_cols: Dict[str, int] = {}
    for col in scan_cols:
        col_mask = df[col].astype(str).str.contains(IRC_PATTERN, na=False)
        if col_mask.any():
            hit_cols[col] = int(col_mask.sum())
            mask |= col_mask

    skipped = int(mask.sum())
    if skipped == 0:
        print("  'As per IRC' check: no rows matched; nothing excluded.")
        audit_event(table, "IRC_EXCLUDE_NONE", 0, f"scanned_columns={scan_cols}")
        return df

    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    skip_path = PREVIEW_DIR / f"{table}_skipped_irc.csv"
    df[mask].to_csv(skip_path, index=False)
    print(
        f"  'As per IRC' rule: {skipped:,} of {len(df):,} rows EXCLUDED from push "
        f"(matched columns: {hit_cols}). Saved to {skip_path.name}"
    )
    audit_event(table, "IRC_ROWS_EXCLUDED", skipped, f"matched_columns={hit_cols}; file={skip_path.name}")
    return df[~mask].copy()


def convert_constrained_values(
    df: pd.DataFrame, table: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Apply VALUE_CONVERSIONS for the target table (e.g. master_ravelling.level 'master' -> 'parent').
    Returns (converted_df, problems) where problems lists values that still violate the allowed set.
    """
    problems: List[str] = []
    rules = VALUE_CONVERSIONS.get(table)
    if not rules or df.empty:
        return df, problems

    df = df.copy()
    for col, rule in rules.items():
        if col not in df.columns:
            continue
        mapping: Dict[str, Any] = rule["map"]
        allowed: Set[str] = rule["allowed"]

        normalized = df[col].astype(str).str.strip().str.lower()
        for src, dst in mapping.items():
            m = normalized == src
            n = int(m.sum())
            if n:
                df.loc[m, col] = dst
                print(f"  Conversion: {table}.{col} '{src}' -> '{dst}' for {n:,} rows")
                audit_event(table, "VALUE_CONVERTED", n, f"{col}: '{src}' -> '{dst}'")

        # Anything still outside the allowed set would be rejected by the CHECK constraint.
        remaining = df[col].dropna().astype(str).str.strip()
        bad_vals = sorted({v for v in remaining.unique() if v.lower() not in {a.lower() for a in allowed}})
        if bad_vals:
            msg = f"{table}.{col} has values outside CHECK-allowed {sorted(allowed)}: {bad_vals}"
            problems.append(msg)
            audit_event(table, "CONSTRAINT_VIOLATION", 0, msg)
    return df, problems


# ---------------------------------------------------------------------------------------------------------------------
# Plan Table (Inspection, Validation & Diffing)
# ---------------------------------------------------------------------------------------------------------------------
def plan_silver_table(
    cur,
    parquet_filename: str,
    target_table: str,
    schema: str,
    source_path: str,
    run_time: str,
    s3_client=None,
    parent_plans: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    print(f"\n{'=' * 90}\n{parquet_filename}  ->  {schema}.{target_table}\nSource: {source_path}\n{'=' * 90}")

    # 1. Read Parquet
    try:
        df = read_parquet_source(source_path, s3_client=s3_client)
    except Exception as e:
        print(f"!! Failed to read Parquet from {source_path}: {e}")
        audit_event(target_table, "READ_FAILED", 0, str(e))
        return None

    print(f"Parquet source: {len(df):,} rows, {len(df.columns)} columns")
    audit_event(target_table, "SOURCE_READ", len(df), source_path)

    # 1a. Business rules: exclude 'As per IRC' rows (*_10m) and convert constrained values
    df = exclude_as_per_irc_rows(df, parquet_filename, target_table)
    df, conversion_problems = convert_constrained_values(df, target_table)

    # 2. Inspect DB Columns
    columns = db_columns(cur, schema, target_table)
    if not columns:
        print(f"!! Table {schema}.{target_table} does not exist in DB - SKIPPED")
        audit_event(target_table, "TABLE_MISSING", 0, "skipped")
        return None

    # Determine matched columns
    pushed = [c for c in df.columns if c in columns and columns[c]["writable"]]
    pq_only = [c for c in df.columns if c not in columns]
    not_writable = [c for c in df.columns if c in columns and not columns[c]["writable"]]
    db_only = [c for c in columns if c not in df.columns]

    print(f"Columns matched and pushed ({len(pushed)}): {pushed}")
    if pq_only:
        print(f"  Parquet columns not in DB (skipped): {pq_only}")
    if not_writable:
        print(f"  DB columns not writable (identity/generated, skipped): {not_writable}")
    if db_only:
        print(f"  DB columns not in Parquet (left as default/NULL): {db_only}")

    # Handle NOT NULL columns with DB defaults where parquet has null/empty
    for col in pushed:
        meta = columns[col]
        if meta["not_null"] and meta["has_default"]:
            d_expr = str(meta.get("default_expr") or "")
            null_mask = df[col].isna() | (df[col].astype(str).str.strip().isin(["", "nan", "None", "null"]))
            if null_mask.any():
                default_val: Any = "0" if meta["type"] in INT_TYPES or "numeric" in meta["type"] else ""
                if "false" in d_expr.lower():
                    default_val = False
                elif "true" in d_expr.lower():
                    default_val = True
                elif m := re.search(r"\'([^\']+)\'", d_expr):
                    default_val = m.group(1)
                elif m := re.search(r"(-?\d+)", d_expr):
                    default_val = int(m.group(1))
                df.loc[null_mask, col] = default_val

    # Check for primary key / matching key
    has_key = KEY in pushed
    if not has_key:
        print(f"  Note: '{KEY}' not in pushed columns. Table will be INSERT-only (no update/match by id).")

    # Handle created_at / updated_at timestamps
    stamps = [c for c in (CREATED_AT, UPDATED_AT) if c in columns and columns[c]["writable"]]
    for col in stamps:
        if col not in pushed:
            pushed.append(col)
            df[col] = None

    update_cols = [c for c in pushed if c not in NOT_UPDATED] if has_key else []
    col_types = {c: columns[c]["type"] for c in pushed}

    if has_key:
        # Drop rows with empty id
        no_id = df[KEY].isna() | (df[KEY].astype(str).str.strip() == "")
        if no_id.any():
            print(f"  !! {int(no_id.sum())} rows with empty {KEY} - dropped from batch")
            audit_event(target_table, "EMPTY_ID_DROPPED", int(no_id.sum()))
            df = df[~no_id]

        # Check duplicates in parquet
        dup = df[KEY].duplicated(keep=False)
        if dup.any():
            print(f"  !! {int(dup.sum())} duplicate IDs found in Parquet - keeping first occurrence")
            audit_event(target_table, "DUPLICATE_ID_DEDUPED", int(dup.sum()))
            df = df.drop_duplicates(subset=[KEY], keep="first")

        in_db = existing_ids(cur, schema, target_table, KEY)
        is_update = df[KEY].astype(str).str.strip().isin(in_db)

        inserts = df[~is_update].copy()
        updates = df[is_update].copy()

        # Stamp timestamps
        if CREATED_AT in stamps and CREATED_AT in inserts.columns:
            inserts[CREATED_AT] = run_time
        if UPDATED_AT in stamps:
            if UPDATED_AT in inserts.columns:
                inserts[UPDATED_AT] = run_time
            if UPDATED_AT in updates.columns:
                updates[UPDATED_AT] = run_time

        print(f"DB currently has {len(in_db):,} rows.")
        print(f"Plan: INSERT {len(inserts):,} new rows, UPDATE {len(updates):,} existing rows.")
    else:
        inserts = df.copy()
        updates = pd.DataFrame(columns=df.columns)
        if CREATED_AT in stamps and CREATED_AT in inserts.columns:
            inserts[CREATED_AT] = run_time
        if UPDATED_AT in stamps and UPDATED_AT in inserts.columns:
            inserts[UPDATED_AT] = run_time
        print(f"Plan: INSERT {len(inserts):,} rows (append-only).")

    plan: Dict[str, Any] = {
        "parquet_filename": parquet_filename,
        "table": target_table,
        "schema": schema,
        "columns": columns,
        "col_types": col_types,
        "pushed": pushed,
        "has_key": has_key,
        "update_cols": update_cols,
        "update_values": ([KEY] + update_cols) if has_key else [],
        "inserts": inserts,
        "updates": updates,
        "ok": True,
    }

    # CHECK-constraint conversion problems (e.g. master_ravelling_level_check)
    if conversion_problems:
        if target_table not in ROW_LEVEL_CONSTRAINT_TABLES:
            plan["ok"] = False
        for msg in conversion_problems:
            print(f"  !! CHECK constraint would fail: {msg}")

    # NOT NULL constraint check
    for col in pushed:
        if columns[col]["not_null"]:
            n_ins = int(inserts[col].isna().sum()) if not inserts.empty else 0
            n_upd = int(updates[col].isna().sum()) if not updates.empty and col in update_cols else 0
            if n_ins or n_upd:
                if target_table not in ROW_LEVEL_CONSTRAINT_TABLES:
                    plan["ok"] = False
                print(f"  !! {col} is NOT NULL in DB, but empty in {n_ins} insert / {n_upd} update rows!")
                audit_event(target_table, "NOT_NULL_VIOLATION", n_ins + n_upd, col)

    required_missing = [c for c in db_only if columns[c]["not_null"] and not columns[c]["has_default"]]
    if required_missing and len(inserts):
        if target_table not in ROW_LEVEL_CONSTRAINT_TABLES:
            plan["ok"] = False
        print(f"  !! DB columns NOT NULL without default and missing from Parquet: {required_missing}")
        audit_event(target_table, "REQUIRED_COLUMNS_MISSING", 0, str(required_missing))

    # Foreign Key validation (aware of both DB and batch parent tables)
    check_and_resolve_foreign_keys(cur, plan, parent_plans=parent_plans)

    # Cast check on sample rows
    if target_table in ROW_LEVEL_CONSTRAINT_TABLES:
        plan["changes"] = pd.DataFrame(columns=[KEY, "column", "db_value", "new_value"])
        print("  Ravelling: individual row constraints will be checked during live writing.")
    else:
        check_inserts_dry_cast(cur, plan)
        diff_updates(cur, plan)

    audit_event(
        target_table,
        "PLAN_READY" if plan["ok"] else "PLAN_FAILED",
        len(inserts) + len(updates),
        f"insert={len(inserts)}; update={len(updates)}",
    )
    return plan


def check_and_resolve_foreign_keys(
    cur, plan: Dict[str, Any], parent_plans: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Validate foreign keys against DB and current batch parents; report missing referenced values."""
    if plan["table"] in ROW_LEVEL_CONSTRAINT_TABLES:
        print("  Ravelling: individual database FK constraints will be checked row by row during writing.")
        return
    if plan["inserts"].empty and plan["updates"].empty:
        return
    rows = pd.concat([plan["inserts"], plan["updates"]])
    parent_plans = parent_plans or {}

    for col, ref_schema, ref_table, ref_col in foreign_keys(cur, plan["schema"], plan["table"]):
        if col not in plan["pushed"]:
            continue
        col_type = plan["columns"][col]["type"]
        vals = {
            clean_cell_value(v, col_type)
            for v in rows[col].dropna().unique()
            if clean_cell_value(v, col_type) is not None
        }
        if not vals:
            continue

        # Check parent tables in current batch
        batch_parent_vals = set()
        if ref_table in parent_plans:
            p_plan = parent_plans[ref_table]
            if ref_col in p_plan["pushed"]:
                batch_rows = pd.concat([p_plan["inserts"], p_plan["updates"]])
                batch_parent_vals = {
                    clean_cell_value(v, p_plan["columns"][ref_col]["type"])
                    for v in batch_rows[ref_col].dropna().unique()
                }

        # Check DB for missing values
        remaining_vals = [v for v in vals if v not in batch_parent_vals]
        missing = []
        if remaining_vals:
            cur.execute(
                sql.SQL(
                    """
                    SELECT x FROM unnest(%s::text[]) AS x 
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {}.{} r WHERE r.{}::text = x
                    ) LIMIT 20;
                    """
                ).format(
                    sql.Identifier(ref_schema), sql.Identifier(ref_table), sql.Identifier(ref_col)
                ),
                (remaining_vals,),
            )
            missing = [r[0] for r in cur.fetchall()]

        if missing:
            # Never silently rewrite source foreign keys. Stop before writing so
            # the owner can correct the parent data or approve an explicit policy.
            plan["ok"] = False
            print(
                f"  !! FK check failed for {col} -> {ref_table}.{ref_col}. "
                f"Missing referenced values (sample): {missing}"
            )
            audit_event(plan["table"], "FK_VIOLATION", len(missing), f"{col} -> {ref_table}.{ref_col}; sample={missing}")
        else:
            print(f"  FK check passed: {col} -> {ref_table}.{ref_col} ({len(vals)} values verified)")


def check_inserts_dry_cast(cur, plan: Dict[str, Any]) -> None:
    """Cast batch of insert values to verify Postgres accepts all types without error."""
    inserts, pushed = plan["inserts"], plan["pushed"]
    if inserts.empty:
        return
    query = sql.SQL("SELECT count(*) FROM (VALUES %s) AS v({})").format(ident_list(pushed)).as_string(cur)
    sample_batch = inserts.head(min(len(inserts), 500))
    raw_rows = rows_of(sample_batch, pushed, plan["col_types"])
    tmpl = values_template(pushed, plan["columns"])
    try:
        execute_values(cur, query, raw_rows, template=tmpl, page_size=len(raw_rows))
        print(f"  Insert type cast check: passed for {len(sample_batch)} sample rows.")
    except psycopg2.Error as e:
        cur.connection.rollback()
        plan["ok"] = False
        print(f"  !! Insert type casting rejected by Postgres: {str(e).strip()}")
        audit_event(plan["table"], "CAST_REJECTED", 0, str(e).strip())


def diff_updates(cur, plan: Dict[str, Any]) -> None:
    """Compare existing DB values vs updated values to identify changed cells."""
    updates, cols = plan["updates"], plan["update_values"]
    compared = [c for c in plan["update_cols"] if c != UPDATED_AT]
    plan["changes"] = pd.DataFrame(columns=[KEY, "column", "db_value", "new_value"])
    if updates.empty or not plan["update_cols"]:
        return

    sample = updates.head(min(len(updates), 200))
    t, v = sql.Identifier("t"), sql.Identifier("v")
    selects = sql.SQL(", ").join(
        sql.SQL("{t}.{c}::text, {v}.{c}::text, {t}.{c} IS DISTINCT FROM {v}.{c}").format(
            t=t, v=v, c=sql.Identifier(c)
        )
        for c in compared
    ) if compared else sql.SQL("NULL")

    query = sql.SQL(
        """
        SELECT {v}.{key}::text, {selects} 
        FROM (VALUES %s) AS v({cols}) 
        JOIN {schema}.{table} AS t ON {t}.{key} = {v}.{key}
        """
    ).format(
        t=t,
        v=v,
        key=sql.Identifier(KEY),
        selects=selects,
        cols=ident_list(cols),
        schema=sql.Identifier(plan["schema"]),
        table=sql.Identifier(plan["table"]),
    ).as_string(cur)

    changes = []
    raw_rows = rows_of(sample, cols, plan["col_types"])
    tmpl = values_template(cols, plan["columns"])
    try:
        for row in execute_values(cur, query, raw_rows, template=tmpl, page_size=len(raw_rows), fetch=True):
            for i, col in enumerate(compared):
                db_val, new_val, changed = row[1 + 3 * i : 4 + 3 * i]
                if changed:
                    changes.append((row[0], col, db_val, new_val))
    except psycopg2.Error as e:
        cur.connection.rollback()
        plan["ok"] = False
        print(f"  !! Update diff check error: {str(e).strip()}")
        audit_event(plan["table"], "DIFF_ERROR", 0, str(e).strip())
        return

    plan["changes"] = pd.DataFrame(changes, columns=[KEY, "column", "db_value", "new_value"])
    if not plan["changes"].empty:
        print(f"  Update diff check: {len(plan['changes'])} cells changed in {len(sample)} sample rows.")


def export_dry_run_preview(plan: Dict[str, Any]) -> None:
    """Save sample previews to dry_run_output/."""
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    table = plan["table"]
    ins_path = PREVIEW_DIR / f"{table}_insert.csv"
    chg_path = PREVIEW_DIR / f"{table}_update_changes.csv"

    plan["inserts"].head(100).to_csv(ins_path, index=False)
    plan["changes"].to_csv(chg_path, index=False)

    if not plan["inserts"].empty:
        print(f"\nSample INSERT rows ({min(SAMPLE_ROWS, len(plan['inserts']))}):")
        print(plan["inserts"][plan["pushed"]].head(SAMPLE_ROWS).to_string(index=False))
    if not plan["changes"].empty:
        print(f"\nSample UPDATE changed cells ({min(SAMPLE_ROWS * 2, len(plan['changes']))}):")
        print(plan["changes"].head(SAMPLE_ROWS * 2).to_string(index=False))


# ---------------------------------------------------------------------------------------------------------------------
# Live Database Write (Single Atomic Transaction)
# ---------------------------------------------------------------------------------------------------------------------

def write_ravelling_table_rowwise(cur, plan: Dict[str, Any]) -> Tuple[int, int]:
    """
    Write Ravelling rows individually.
    A failed row is rolled back to its savepoint; other rows continue.
    """
    schema = plan["schema"]
    table = plan["table"]
    rejected = []
    updated = 0
    inserted = 0

    target = sql.SQL("{}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )

    def record_rejection(row, operation, error):
        item = row.to_dict()
        item["_operation"] = operation
        item["_error"] = str(error).strip()
        rejected.append(item)
        audit_event(
            table,
            "ROW_REJECTED",
            1,
            f"operation={operation}; error={str(error).strip()}"
        )

    def process_row(row, operation):
        nonlocal updated, inserted

        cur.execute("SAVEPOINT ravelling_row")
        try:
            if operation == "UPDATE":
                cols = plan["update_values"]
                values = rows_of(
                    pd.DataFrame([row]),
                    cols,
                    plan["col_types"]
                )[0]

                row_values = dict(zip(cols, values))
                set_cols = plan["update_cols"]

                if not set_cols:
                    cur.execute("RELEASE SAVEPOINT ravelling_row")
                    return

                query = sql.SQL(
                    "UPDATE {} SET {} WHERE {} = %s"
                ).format(
                    target,
                    sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(c))
                        for c in set_cols
                    ),
                    sql.Identifier(KEY)
                )

                params = [row_values[c] for c in set_cols]
                params.append(row_values[KEY])
                cur.execute(query, params)

                if cur.rowcount:
                    updated += cur.rowcount

            else:
                cols = plan["pushed"]
                values = rows_of(
                    pd.DataFrame([row]),
                    cols,
                    plan["col_types"]
                )[0]

                query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES ({})"
                ).format(
                    target,
                    ident_list(cols),
                    sql.SQL(", ").join(sql.Placeholder() for _ in cols)
                )

                cur.execute(query, values)
                inserted += cur.rowcount

            cur.execute("RELEASE SAVEPOINT ravelling_row")

        except (
            psycopg2.IntegrityError,
            psycopg2.DataError,
            ValueError,
            TypeError,
            OverflowError,
        ) as exc:
            cur.execute("ROLLBACK TO SAVEPOINT ravelling_row")
            cur.execute("RELEASE SAVEPOINT ravelling_row")
            record_rejection(row, operation, exc)

    # Process existing rows first, preserving the original update/insert plan.
    for _, row in plan["updates"].iterrows():
        process_row(row, "UPDATE")

    for _, row in plan["inserts"].iterrows():
        process_row(row, "INSERT")

    if rejected:
        PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
        rejected_path = PREVIEW_DIR / f"{table}_constraint_rejected.csv"
        pd.DataFrame(rejected).to_csv(rejected_path, index=False)
        print(
            f"  Rejected {len(rejected):,} rows. "
            f"Details saved to: {rejected_path}"
        )
    else:
        print("  No Ravelling rows rejected by database constraints.")

    print(
        f"  Row-wise result: UPDATED {updated:,}, "
        f"INSERTED {inserted:,}, REJECTED {len(rejected):,}"
    )

    return updated, inserted



def write_silver_table(cur, plan: Dict[str, Any]) -> Tuple[int, int]:
    """Execute INSERT and UPDATE statements for one table. Returns (updated_rows, inserted_rows)."""
    if plan["table"] in ROW_LEVEL_CONSTRAINT_TABLES:
        return write_ravelling_table_rowwise(cur, plan)
    schema, table, pushed = plan["schema"], plan["table"], plan["pushed"]
    update_cols, update_values = plan["update_cols"], plan["update_values"]
    target = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    updated = inserted = 0

    # 1. UPDATE existing rows
    if plan["has_key"] and not plan["updates"].empty and update_cols:
        query = sql.SQL(
            """
            UPDATE {target} AS t 
            SET {sets} 
            FROM (VALUES %s) AS v({cols}) 
            WHERE t.{key} = v.{key}
            """
        ).format(
            target=target,
            cols=ident_list(update_values),
            key=sql.Identifier(KEY),
            sets=sql.SQL(", ").join(
                sql.SQL("{c} = v.{c}").format(c=sql.Identifier(c)) for c in update_cols
            ),
        ).as_string(cur)

        tmpl = values_template(update_values, plan["columns"])
        for part in chunks(rows_of(plan["updates"], update_values, plan["col_types"])):
            execute_values(cur, query, part, template=tmpl, page_size=len(part))
            updated += cur.rowcount

    # 2. INSERT new rows
    if not plan["inserts"].empty:
        query = sql.SQL("INSERT INTO {target} ({cols}) VALUES %s").format(
            target=target, cols=ident_list(pushed)
        ).as_string(cur)

        tmpl = values_template(pushed, plan["columns"])
        for part in chunks(rows_of(plan["inserts"], pushed, plan["col_types"])):
            execute_values(cur, query, part, template=tmpl, page_size=len(part))
            inserted += cur.rowcount

    return updated, inserted


# ---------------------------------------------------------------------------------------------------------------------
# Input Parsing
# ---------------------------------------------------------------------------------------------------------------------
def parse_survey_input(input_text: str) -> Tuple[str, str, str, Optional[str]]:
    """
    Parse UPC, cycle, phase, and optional zone from a text string or line.
    Handles formats like:
      'upc  cycle  phase'
      'N/01002/01001/GJ 1 1'
    """
    lines = [ln.strip() for ln in input_text.strip().splitlines() if ln.strip()]
    target_line = None
    for line in lines:
        if line.lower().startswith("upc"):
            continue
        target_line = line
        break

    if not target_line:
        raise ValueError(f"Could not parse valid UPC line from input: {input_text}")

    parts = [p.strip() for p in re.split(r"[\s,\t]+", target_line) if p.strip()]
    if len(parts) < 3:
        raise ValueError(f"Expected at least 3 values (upc cycle phase), got: {parts}")

    upc = parts[0]
    cycle = parts[1]
    phase = parts[2]
    zone = parts[3] if len(parts) > 3 else None
    return upc, cycle, phase, zone


# ---------------------------------------------------------------------------------------------------------------------
# Main CLI & Workflow
# ---------------------------------------------------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Push Silver Parquet tables (S3 or local) into PostgreSQL DB (dhms_staging)."
    )
    p.add_argument("--upc", help="UPC code, e.g. 'N/01002/01001/GJ'")
    p.add_argument("--cycle", help="Cycle number, e.g. 1")
    p.add_argument("--phase", help="Phase number, e.g. 1")
    p.add_argument("--zone", help="Zone code, e.g. 'B' (auto-resolved if omitted)")
    p.add_argument("--input", help="Direct string input with 'upc cycle phase', e.g. 'N/01002/01001/GJ 1 1'")
    p.add_argument("--input-file", help="Path to text/csv file containing upc, cycle, phase")
    p.add_argument("--s3-root", default=None, help=f"S3 root prefix (default: {DEFAULT_S3_ROOT})")
    p.add_argument("--local-dir", help="Local directory containing parquet files instead of S3")
    p.add_argument("--only", action="append", help="Push only this table name or parquet file (repeatable)")
    p.add_argument(
        "--dry-run",
        choices=["true", "false"],
        default=None,
        help=f"true: report only, false: write to DB (default: {DEFAULT_DRY_RUN})",
    )
    return p.parse_args()


def main():
    args = parse_args()
    dry_run = DEFAULT_DRY_RUN if args.dry_run is None else (args.dry_run == "true")
    load_env()
    settings = db_settings()
    schema = os.environ.get("DB_SCHEMA", "public")
    s3_root = (args.s3_root or os.environ.get("S3_SILVER_ROOT")
               or os.environ.get("SILVER_S3_ROOT") or DEFAULT_S3_ROOT)
    run_time = datetime.now(timezone.utc).isoformat(sep=" ")

    # Parse UPC, Cycle, Phase
    upc, cycle, phase, zone = None, None, None, args.zone
    if args.input_file:
        upc, cycle, phase, z = parse_survey_input(Path(args.input_file).read_text())
        zone = zone or z
    elif args.input:
        upc, cycle, phase, z = parse_survey_input(args.input)
        zone = zone or z
    elif args.upc:
        upc, cycle, phase = args.upc, args.cycle or "1", args.phase or "1"
    elif not args.local_dir:
        upc, cycle, phase = "N/02015/03002/GJ", "1", "1"

    print("=" * 90)
    print("SILVER PARQUET TO DATABASE PUSH PIPELINE")
    print(f"Mode: {'DRY RUN (Read & validate only)' if dry_run else 'LIVE RUN (Writing to Database)'}")
    print(f"Target DB: {settings['user']}@{settings['host']}:{settings['port']}/{settings['dbname']} ({schema})")
    if upc:
        print(f"Survey Target: UPC={upc} | Cycle={cycle} | Phase={phase} | Zone={zone or '(auto-detect)'}")
    print("=" * 90)
    audit_event("-", "RUN_START", 0, f"mode={'DRY_RUN' if dry_run else 'LIVE'}; upc={upc}; cycle={cycle}; phase={phase}")

    # Database connection
    conn = psycopg2.connect(connect_timeout=15, application_name="silver_parquet_db_push", **settings)

    try:
        with conn.cursor() as cur:
            # Auto-detect Zone from DB if needed
            if upc and not zone:
                zone = resolve_zone_from_db(cur, upc)
                if zone:
                    print(f"Resolved Zone from DB: {zone}")

            # Locate Parquet files (Local or S3)
            s3_client = None
            if args.local_dir:
                print(f"Scanning local directory for Silver Parquet files: {args.local_dir}")
                parquet_map = locate_local_parquet_files(Path(args.local_dir))
            else:
                s3_client = get_s3_client()
                parquet_map = locate_s3_parquet_files(
                    s3_client=s3_client,
                    s3_root=s3_root,
                    upc=upc,
                    phase=phase,
                    cycle=cycle,
                    zone=zone,
                )

            # Check status of expected tables vs discovered parquets
            print("\nTable Status & Mapping Assessment:")
            print("-" * 90)
            plans = []
            plans_by_table: Dict[str, Dict[str, Any]] = {}

            for pq_file, primary_tab, fallbacks in SILVER_TABLE_SPECS:
                if args.only:
                    if not any(o in (pq_file, primary_tab, *fallbacks) for o in args.only):
                        continue

                source_path = parquet_map.get(pq_file)
                target_table = None
                if primary_tab is None:
                    print(f"  {pq_file:36s} -> DB: {'NO MAPPING':28s} | File: {'FOUND' if source_path else 'MISSING ON S3'}")
                    print("    --> Skipping: schema-check mapping explicitly has no target DB table.")
                    audit_event(pq_file, "NO_DB_MAPPING_SKIPPED", 0)
                    continue
                if db_table_exists(cur, schema, primary_tab):
                    target_table = primary_tab
                else:
                    for fb in fallbacks:
                        if db_table_exists(cur, schema, fb):
                            target_table = fb
                            break

                status_tab = target_table if target_table else f"MISSING ({primary_tab})"
                status_file = "FOUND" if source_path else "MISSING ON S3"
                print(f"  {pq_file:36s} -> DB: {status_tab:28s} | File: {status_file}")

                if not target_table:
                    print(f"    --> Skipping: target table '{primary_tab}' does not exist in DB schema '{schema}'.")
                    audit_event(primary_tab, "SKIPPED_TABLE_NOT_IN_DB", 0)
                    continue
                if not source_path:
                    print(f"    --> Skipping: Parquet file '{pq_file}' not found.")
                    audit_event(target_table, "SKIPPED_FILE_NOT_FOUND", 0, pq_file)
                    continue

                # Plan table execution with parent-plan context for FK resolution
                plan = plan_silver_table(
                    cur=cur,
                    parquet_filename=pq_file,
                    target_table=target_table,
                    schema=schema,
                    source_path=source_path,
                    run_time=run_time,
                    s3_client=s3_client,
                    parent_plans=plans_by_table,
                )
                if plan is not None:
                    export_dry_run_preview(plan)
                    plans.append(plan)
                    plans_by_table[target_table] = plan

            # Rollback read transaction
            conn.rollback()

            bad_plans = [p["table"] for p in plans if not p["ok"]]
            print("\n" + "=" * 90)
            print("SUMMARY:")
            for p in plans:
                print(
                    f"  {p['table']:28s}: would INSERT {len(p['inserts']):,} rows, "
                    f"UPDATE {len(p['updates']):,} rows "
                    f"({p['changes'][KEY].nunique() if 'changes' in p and not p['changes'].empty else 0:,} with changed cells)"
                )
            print("=" * 90)

            if dry_run:
                if bad_plans:
                    print(f"DRY RUN completed with ERRORS on tables: {bad_plans} (see !! above).")
                    audit_event("-", "RUN_END_DRY_RUN_ERRORS", 0, str(bad_plans))
                    return 1
                print("DRY RUN completed successfully. No changes made to DB.")
                print("To write changes to the database, run with: --dry-run false")
                audit_event("-", "RUN_END_DRY_RUN_OK", 0)
                return 0

            # Live run
            if bad_plans:
                print(f"Write aborted: validation failed for {bad_plans}. DB remains untouched.")
                audit_event("-", "RUN_ABORTED_VALIDATION", 0, str(bad_plans))
                return 1

            if not plans:
                print("No plans to execute.")
                audit_event("-", "RUN_END_NO_PLANS", 0)
                return 0

            print("\nWriting all tables in ONE atomic transaction...")
            for p in plans:
                updated, inserted = write_silver_table(cur, p)
                print(f"  {p['table']:28s}: UPDATED {updated:,} rows, INSERTED {inserted:,} rows")
                audit_event(p["table"], "WRITTEN", updated + inserted, f"updated={updated}; inserted={inserted}")

            conn.commit()
            print("Transaction COMMITTED successfully.")
            audit_event("-", "RUN_END_COMMITTED", 0)
            return 0

    except Exception as e:
        conn.rollback()
        print(f"\n!! Exception occurred: {e}. Transaction ROLLED BACK, DB unchanged.")
        audit_event("-", "RUN_ROLLED_BACK", 0, str(e))
        raise
    finally:
        write_audit_log(run_time, dry_run)
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
