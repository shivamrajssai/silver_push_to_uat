"""Consolidated S3/local Silver Parquet to PostgreSQL migration utility."""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


DEFAULT_S3_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver"
PAGE_SIZE = 1000
KEY = "id"
CREATED_AT = "created_at"
UPDATED_AT = "updated_at"
NOT_UPDATED = {KEY, CREATED_AT}
INT_TYPES = {"smallint", "integer", "bigint"}
IRC_PATTERN = re.compile(r"as\s*per\s*irc", re.IGNORECASE)

# source filename -> (target table, fallback tables)
TABLE_MAP: Dict[str, Optional[Tuple[str, ...]]] = {
    "geometry_child.parquet": ("geometry_child",),
    "geometry_parent.parquet": ("geometry_parent",),
    "master_chainage.parquet": None,
    "master_cracking_1000m_lane.parquet": None,
    "master_cracking_10m.parquet": ("master_cracking_10m", "master_cracking_child_10m"),
    "master_cracking_as_per_ca.parquet": ("master_cracking_as_per_ca",),
    "master_pothole.parquet": ("master_pothole",),
    "master_ravelling_1000m_lane.parquet": None,
    "master_ravelling_10m.parquet": ("master_ravelling_10m",),
    "master_ravelling.parquet": ("master_ravelling",),
    "master_roughness_1000m_lane.parquet": None,
    "master_roughness_as_per_ca.parquet": ("master_roughness_as_per_ca",),
    "master_roughness_child_10m.parquet": ("master_roughness_child_10m",),
    "master_roughness_parent.parquet": ("master_roughness_parent",),
    "master_rutting_1000m_lane.parquet": None,
    "master_rutting_as_per_ca.parquet": ("master_rutting_as_per_ca",),
    "master_rutting_child_10m.parquet": ("master_rutting_child_10m",),
    "master_rutting_parent.parquet": ("master_rutting_parent",),
}

# Explicit value corrections for database CHECK constraints.
VALUE_CONVERSIONS: Dict[str, Dict[str, Any]] = {
    "master_ravelling": {"level": {"master": "parent", "allowed": {"parent"}}},
    "master_pothole": {
        "level": {"force": "child_10m", "allowed": {"child_10m"}},
        "defect_base": {"force": "pothole", "allowed": {"pothole"}},
    },
}


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_schema: str
    s3_root: str
    aws_region: str


@dataclass
class Plan:
    source_file: str
    target_table: str
    source_path: str
    columns: Dict[str, Dict[str, Any]]
    pushed: List[str]
    col_types: Dict[str, str]
    has_key: bool
    update_cols: List[str]
    update_values: List[str]
    inserts: pd.DataFrame
    updates: pd.DataFrame
    changes: pd.DataFrame
    ok: bool


def load_settings() -> Settings:
    env_file = Path(__file__).resolve().parent / ".env"
    if load_dotenv:
        load_dotenv(env_file, override=False)

    missing = [name for name in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
            + ". Copy .env.example to .env and configure one PostgreSQL target."
        )

    return Settings(
        db_host=os.environ["DB_HOST"],
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.environ["DB_NAME"],
        db_user=os.environ["DB_USER"],
        db_password=os.environ["DB_PASSWORD"],
        db_schema=os.getenv("DB_SCHEMA", "public"),
        s3_root=os.getenv("S3_SILVER_ROOT", DEFAULT_S3_ROOT),
        aws_region=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-south-1")),
    )


def db_connection(settings: Settings):
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=15,
        application_name="silver_parquet_to_uat_push",
    )


def get_s3_client(settings: Settings):
    return boto3.client("s3", region_name=settings.aws_region)


def parse_survey_input(text: str) -> Tuple[str, str, str, Optional[str]]:
    for line in (line.strip() for line in text.splitlines() if line.strip()):
        if line.lower().startswith("upc"):
            continue
        parts = [part.strip() for part in re.split(r"[\s,\t]+", line) if part.strip()]
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2], parts[3] if len(parts) > 3 else None
    raise ValueError("Expected UPC, cycle, and phase: 'N/01002/01001/GJ 1 1'")


def parse_upc_variations(upc: str) -> List[str]:
    clean = upc.strip()
    slash = clean.replace("_", "/").replace("-", "/")
    parts = [part for part in slash.split("/") if part]
    if len(parts) == 4:
        return ["/".join(parts), "_".join(parts), "-".join(parts)]
    return [clean, clean.replace("/", "_"), clean.replace("/", "-")]


def normalize_path(path: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", path.lower()).strip("_")


def discover_s3_files(
    s3,
    settings: Settings,
    upc: str,
    cycle: str,
    phase: str,
    zone: Optional[str],
) -> Dict[str, str]:
    if not settings.s3_root.startswith("s3://"):
        raise ValueError("S3_SILVER_ROOT must start with s3://")

    bucket_and_prefix = settings.s3_root[5:].split("/", 1)
    bucket = bucket_and_prefix[0]
    prefix = bucket_and_prefix[1].strip("/") if len(bucket_and_prefix) == 2 else ""
    upcs = {normalize_path(value) for value in parse_upc_variations(upc)}
    found: Dict[str, Tuple[str, str]] = {}

    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if filename not in TABLE_MAP:
                continue
            normalized = normalize_path(key)
            if not any(normalized_upc in normalized for normalized_upc in upcs):
                continue
            if f"cycle_{cycle}" not in normalized and f"c_{cycle}" not in normalized:
                continue
            if f"phase_{phase}" not in normalized and f"p_{phase}" not in normalized:
                continue
            if zone and f"zone_{normalize_path(zone)}" not in normalized:
                continue
            candidate = (key, obj.get("LastModified", datetime.min).isoformat())
            if filename not in found or candidate[1] > found[filename][1]:
                found[filename] = candidate

    return {filename: f"s3://{bucket}/{key}" for filename, (key, _) in found.items()}


def discover_local_files(local_dir: Path) -> Dict[str, str]:
    if not local_dir.is_dir():
        raise ValueError(f"Local Parquet directory does not exist: {local_dir}")
    found: Dict[str, str] = {}
    for root, _, files in os.walk(local_dir):
        for filename in files:
            if filename in TABLE_MAP and filename not in found:
                found[filename] = str(Path(root) / filename)
    return found


def read_parquet(source: str, s3=None) -> pd.DataFrame:
    if source.startswith("s3://"):
        if s3 is None:
            raise RuntimeError("S3 client is required to read an S3 source")
        bucket, key = source[5:].split("/", 1)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return pd.read_parquet(io.BytesIO(body))
    return pd.read_parquet(source)


def resolve_zone(cur, upc: str) -> Optional[str]:
    values = parse_upc_variations(upc)
    cur.execute(
        """
        SELECT mz.zone_code
        FROM master_upc_project p
        JOIN master_zone mz ON p.nsv_zone_id = mz.id
        WHERE p.upc = ANY(%s)
        LIMIT 1
        """,
        (values,),
    )
    row = cur.fetchone()
    return str(row[0]).strip() if row and row[0] else None


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cur.fetchone() is not None


def db_columns(cur, schema: str, table: str) -> Dict[str, Dict[str, Any]]:
    cur.execute(
        """
        SELECT a.attname,
               format_type(a.atttypid, a.atttypmod),
               a.attnotnull,
               a.atthasdef,
               pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
               a.attgenerated = '' AND a.attidentity <> 'a'
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE n.nspname=%s AND c.relname=%s
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    return {
        name: {
            "type": typ,
            "not_null": not_null,
            "has_default": has_default,
            "default_expr": default_expr,
            "writable": writable,
        }
        for name, typ, not_null, has_default, default_expr, writable in cur.fetchall()
    }


def is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "none", "nat", "null"}
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def pg_array(value: Any) -> Optional[List[Any]]:
    if is_null(value):
        return None
    if isinstance(value, (list, tuple, set)):
        return [item.item() if hasattr(item, "item") else item for item in value if not is_null(item)]
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return pg_array(converted)

    text = str(value).strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        import json
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [item for item in parsed if not is_null(item)]
        except Exception:
            pass
    if text.startswith("{") and text.endswith("}"):
        text = text[1:-1]
        if not text:
            return []
    if "," in text:
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text]


def clean_cell(value: Any, col_type: str) -> Any:
    if is_null(value):
        return None
    normalized = col_type.lower()
    if normalized.endswith("[]"):
        return pg_array(value)
    if normalized.startswith("json"):
        if isinstance(value, (dict, list, tuple)):
            import json
            return json.dumps(value, default=str)
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        value = value.item()
    text = str(value).strip()
    if normalized in INT_TYPES:
        number = float(text)
        if not number.is_integer():
            raise ValueError(f"Non-integral value {text!r} for {col_type}")
        return str(int(number))
    if normalized == "uuid":
        return str(uuid.UUID(text))
    if normalized == "boolean":
        lowered = text.lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
        raise ValueError(f"Cannot convert {text!r} to boolean")
    return text


def ident_list(columns: Iterable[str]) -> sql.Composable:
    return sql.SQL(", ").join(sql.Identifier(column) for column in columns)


def values_template(columns: Iterable[str], metadata: Dict[str, Dict[str, Any]]) -> str:
    columns = list(columns)
    return "(" + ", ".join(f"%s::{metadata[column]['type']}" for column in columns) + ")"


def remove_irc_rows(df: pd.DataFrame, table: str, preview_dir: Path) -> pd.DataFrame:
    if df.empty:
        return df
    override = [item.strip() for item in os.getenv("IRC_FILTER_COLUMNS", "").split(",") if item.strip()]
    scan_columns = override or [
        column for column in df.columns
        if df[column].dtype == object or str(df[column].dtype).startswith("string")
    ]
    scan_columns = [column for column in scan_columns if column in df.columns]
    mask = pd.Series(False, index=df.index)
    for column in scan_columns:
        mask |= df[column].astype(str).str.contains(IRC_PATTERN, na=False)

    excluded = int(mask.sum())
    if excluded:
        preview_dir.mkdir(parents=True, exist_ok=True)
        path = preview_dir / f"{table}_as_per_irc_excluded.csv"
        df.loc[mask].to_csv(path, index=False)
        print(f"  Removed {excluded:,} 'As per IRC' rows; preview: {path}")
    return df.loc[~mask].copy()


def apply_value_conversions(df: pd.DataFrame, table: str) -> Tuple[pd.DataFrame, List[str]]:
    rules = VALUE_CONVERSIONS.get(table)
    if not rules or df.empty:
        return df, []
    df = df.copy()
    problems: List[str] = []
    for column, rule in rules.items():
        if column not in df.columns:
            continue
        if "force" in rule:
            df[column] = rule["force"]
        else:
            normalized = df[column].astype(str).str.strip().str.lower()
            for source, target in rule.items():
                if source == "allowed":
                    continue
                mask = normalized == source
                if mask.any():
                    df.loc[mask, column] = target
        allowed = {value.lower() for value in rule["allowed"]}
        remaining = df[column].dropna().astype(str).str.strip().str.lower()
        bad = sorted(set(remaining.unique()) - allowed)
        if bad:
            problems.append(f"{table}.{column} contains values outside {sorted(rule['allowed'])}: {bad}")
    return df, problems


def foreign_key_check(cur, plan: Plan, schema: str, parent_plans: Dict[str, Plan]) -> List[str]:
    if plan.inserts.empty and plan.updates.empty:
        return []
    cur.execute(
        """
        SELECT a.attname, rn.nspname, rc.relname, ra.attname
        FROM pg_constraint k
        JOIN pg_class c ON c.oid = k.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class rc ON rc.oid = k.confrelid
        JOIN pg_namespace rn ON rn.oid = rc.relnamespace
        JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=k.conkey[1]
        JOIN pg_attribute ra ON ra.attrelid=k.confrelid AND ra.attnum=k.confkey[1]
        WHERE k.contype='f' AND n.nspname=%s AND c.relname=%s AND cardinality(k.conkey)=1
        """,
        (schema, plan.target_table),
    )
    failures: List[str] = []
    combined = pd.concat([plan.inserts, plan.updates], ignore_ignore=True) if hasattr(pd, "concat") else pd.concat([plan.inserts, plan.updates])
    for column, reference_schema, reference_table, reference_column in cur.fetchall():
        if column not in plan.pushed:
            continue
        type_name = plan.columns[column]["type"]
        values = {
            clean_cell(value, type_name)
            for value in combined[column].dropna().unique()
            if not is_null(value)
        }
        if not values:
            continue

        batch_values: Set[Any] = set()
        if reference_table in parent_plans:
            parent = parent_plans[reference_table]
            if reference_column in parent.pushed:
                parent_rows = pd.concat([parent.inserts, parent.updates])
                parent_type = parent.columns[reference_column]["type"]
                batch_values = {
                    clean_cell(value, parent_type)
                    for value in parent_rows[reference_column].dropna().unique()
                }

        remaining = [value for value in values if value not in batch_values]
        missing: List[Any] = []
        if remaining:
            cur.execute(
                sql.SQL("""
                    SELECT x FROM unnest(%s::text[]) AS x
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {}.{} r WHERE r.{}::text=x
                    ) LIMIT 20
                """).format(
                    sql.Identifier(reference_schema),
                    sql.Identifier(reference_table),
                    sql.Identifier(reference_column),
                ),
                (remaining,),
            )
            missing = [row[0] for row in cur.fetchall()]

        if missing:
            failures.append(
                f"Foreign key failure: {schema}.{plan.target_table}.{column} -> "
                f"{reference_schema}.{reference_table}.{reference_column}; missing values: {missing}"
            )
    return failures


def plan_table(
    cur,
    s3,
    settings: Settings,
    source_file: str,
    target_table: str,
    source_path: str,
    run_time: str,
    preview_dir: Path,
    parent_plans: Dict[str, Plan],
) -> Optional[Plan]:
    print(f"\n{source_file} -> {settings.db_schema}.{target_table}")
    print(f"Source: {source_path}")
    df = read_parquet(source_path, s3)
    print(f"Read {len(df):,} source rows and {len(df.columns)} columns")

    df = remove_irc_rows(df, target_table, preview_dir)
    df, conversion_problems = apply_value_conversions(df, target_table)
    if df.empty:
        print("  No rows remain after filtering")

    columns = db_columns(cur, settings.db_schema, target_table)
    if not columns:
        print(f"  SKIP: target table {settings.db_schema}.{target_table} does not exist")
        return None

    pushed = [column for column in df.columns if column in columns and columns[column]["writable"]]
    for column in pushed:
        metadata = columns[column]
        if metadata["not_null"] and metadata["has_default"]:
            null_mask = df[column].isna() | df[column].astype(str).str.strip().isin({"", "nan", "None", "null"})
            if null_mask.any():
                default_expression = str(metadata.get("default_expr") or "")
                default_value: Any = 0
                if "false" in default_expression.lower():
                    default_value = False
                elif "true" in default_expression.lower():
                    default_value = True
                elif match := re.search(r"'([^']+)'.*$", default_expression):
                    default_value = match.group(1)
                df.loc[null_mask, column] = default_value

    if KEY not in pushed:
        has_key = False
        inserts = df.copy()
        updates = pd.DataFrame(columns=df.columns)
    else:
        has_key = True
        df = df[df[KEY].notna()].copy()
        df = df[df[KEY].astype(str).str.strip() != ""]
        df = df.drop_duplicates(subset=[KEY], keep="first")

        cur.execute(
            sql.SQL("SELECT {}::text FROM {}.{}").format(
                sql.Identifier(KEY),
                sql.Identifier(settings.db_schema),
                sql.Identifier(target_table),
            )
        )
        existing = {str(row[0]).strip() for row in cur.fetchall() if row[0] is not None}
        is_update = df[KEY].astype(str).str.strip().isin(existing)
        inserts = df.loc[~is_update].copy()
        updates = df.loc[is_update].copy()

    stamps = [column for column in (CREATED_AT, UPDATED_AT) if column in columns and columns[column]["writable"]]
    for column in stamps:
        if column not in pushed:
            pushed.append(column)
            df[column] = None
        if column in inserts.columns:
            inserts[column] = run_time
        if column in updates.columns:
            updates[column] = run_time

    update_cols = [column for column in pushed if column not in NOT_UPDATED] if has_key else []
    update_values = ([KEY] + update_cols) if has_key else []
    col_types = {column: columns[column]["type"] for column in pushed}

    plan = Plan(
        source_file=source_file,
        target_table=target_table,
        source_path=source_path,
        columns=columns,
        pushed=pushed,
        col_types=col_types,
        has_key=has_key,
        update_cols=update_cols,
        update_values=update_values,
        inserts=inserts,
        updates=updates,
        changes=pd.DataFrame(columns=[KEY, "column", "db_value", "new_value"]),
        ok=True,
    )

    problems: List[str] = []
    problems.extend(conversion_problems)

    for column in pushed:
        if not columns[column]["not_null"]:
            continue
        insert_count = int(inserts[column].isna().sum()) if column in inserts.columns else 0
        update_count = int(updates[column].isna().sum()) if column in updates.columns and column in update_cols else 0
        if insert_count or update_count:
            problems.append(
                f"NOT NULL violation: {target_table}.{column}; "
                f"inserts={insert_count}, updates={update_count}"
            )

    db_only = [column for column in columns if column not in df.columns]
    required_missing = [
        column for column in db_only
        if columns[column]["not_null"] and not columns[column]["has_default"]
    ]
    if required_missing and not inserts.empty:
        problems.append(
            f"Required DB columns missing from Parquet and without defaults: {required_missing}"
        )

    problems.extend(foreign_key_check(cur, plan, settings.db_schema, parent_plans))

    if problems:
        plan.ok = False
        for problem in problems:
            print(f"  BLOCKED: {problem}")
    else:
        print(f"  VALID: INSERT {len(inserts):,}, UPDATE {len(updates):,}")

    return plan


def rows_of(df: pd.DataFrame, columns: List[str], col_types: Dict[str, str]) -> List[Tuple]:
    return [
        tuple(clean_cell(value, col_types[column]) for column, value in zip(columns, row))
        for row in df[columns].itertuples(index=False, name=None)
    ]


def chunks(items: List[Any], size: int = PAGE_SIZE):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def write_table(cur, plan: Plan, schema: str) -> Tuple[int, int]:
    updated = inserted = 0
    target = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(plan.target_table))

    if plan.has_key and not plan.updates.empty and plan.update_cols:
        query = sql.SQL(
            "UPDATE {target} AS t SET {sets} FROM (VALUES %s) AS v({columns}) "
            "WHERE t.{key}=v.{key}"
        ).format(
            target=target,
            columns=ident_list(plan.update_values),
            key=sql.Identifier(KEY),
            sets=sql.SQL(", ").join(
                sql.SQL("{column}=v.{column}").format(column=sql.Identifier(column))
                for column in plan.update_cols
            ),
        ).as_string(cur)
        template = values_template(plan.update_values, plan.columns)
        raw_rows = rows_of(plan.updates, plan.update_values, plan.col_types)
        for batch in chunks(raw_rows):
            execute_values(cur, query, batch, template=template, page_size=len(batch))
            updated += cur.rowcount

    if not plan.inserts.empty:
        query = sql.SQL("INSERT INTO {target} ({columns}) VALUES %s").format(
            target=target,
            columns=ident_list(plan.pushed),
        ).as_string(cur)
        template = values_template(plan.pushed, plan.columns)
        raw_rows = rows_of(plan.inserts, plan.pushed, plan.col_types)
        for batch in chunks(raw_rows):
            execute_values(cur, query, batch, template=template, page_size=len(batch))
            inserted += cur.rowcount

    return updated, inserted


def write_audit(preview_dir: Path, table: str, event: str, detail: str) -> None:
    preview_dir.mkdir(parents=True, exist_ok=True)
    path = preview_dir / "silver_push_audit.csv"
    row = pd.DataFrame([{
        "logged_at": datetime.now(timezone.utc).isoformat(),
        "table": table,
        "event": event,
        "detail": detail,
    }])
    if path.exists():
        row.to_csv(path, mode="a", header=False, index=False)
    else:
        row.to_csv(path, index=False)


def check_schema(cur, s3, settings: Settings, files: Dict[str, str]) -> int:
    failures = 0
    print("Schema compatibility check")
    for filename, mapping in TABLE_MAP.items():
        if mapping is None:
            print(f"{filename}: SKIP (not pushed)")
            continue
        if filename not in files:
            print(f"{filename}: MISSING")
            continue
        df = read_parquet(files[filename], s3)
        target = next((table for table in mapping if table_exists(cur, settings.db_schema, table)), None)
        if not target:
            print(f"{filename}: target table missing")
            failures += 1
            continue
        db = db_columns(cur, settings.db_schema, target)
        parquet_columns = set(df.columns)
        db_columns_set = set(db)
        missing_db = sorted(db_columns_set - parquet_columns)
        missing_parquet = sorted(parquet_columns - db_columns_set)
        print(f"{filename} -> {target}: match={len(parquet_columns & db_columns_set)}, DB-only={missing_db}, Parquet-only={missing_parquet}")
        if missing_db or missing_parquet:
            failures += 1
    return failures


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Push Silver Parquet datasets to PostgreSQL")
    parser.add_argument("--upc")
    parser.add_argument("--cycle")
    parser.add_argument("--phase")
    parser.add_argument("--zone")
    parser.add_argument("--input", help="UPC cycle phase, for example 'N/01002/01001/GJ 1 1'")
    parser.add_argument("--input-file", type=Path)
    parser.add_argument("--local-dir", type=Path, help="Read Parquet files from a local directory")
    parser.add_argument("--only", action="append", help="Limit processing to a source or target table name")
    parser.add_argument("--dry-run", choices=["true", "false"], default="true")
    parser.add_argument("--check-schema", action="store_true")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        settings = load_settings()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    upc = cycle = phase = zone = None
    if args.input_file:
        upc, cycle, phase, zone = parse_survey_input(args.input_file.read_text())
    elif args.input:
        upc, cycle, phase, zone = parse_survey_input(args.input)
    elif args.upc:
        upc = args.upc
        cycle = args.cycle or "1"
        phase = args.phase or "1"
        zone = args.zone
    elif not args.local_dir:
        print("Supply --upc/--cycle/--phase, --input, --input-file, or --local-dir", file=sys.stderr)
        return 2

    if args.zone:
        zone = args.zone

    s3 = None
    conn = db_connection(settings)
    preview_dir = Path(__file__).resolve().parent / "dry_run_output"

    try:
        with conn.cursor() as cur:
            if upc and not zone:
                zone = resolve_zone(cur, upc)

            if args.local_dir:
                files = discover_local_files(args.local_dir)
            else:
                s3 = get_s3_client(settings)
                files = discover_s3_files(s3, settings, upc, cycle, phase, zone)

            if args.check_schema:
                result = check_schema(cur, s3, settings, files)
                conn.rollback()
                return 1 if result else 0

            plans: List[Plan] = []
            plans_by_table: Dict[str, Plan] = {}

            print("Table status:")
            for filename, mapping in TABLE_MAP.items():
                if mapping is None:
                    print(f"  {filename}: NOT PUSHED")
                    continue
                if args.only and not any(value in (filename, *mapping) for value in args.only):
                    continue
                source_path = files.get(filename)
                if not source_path:
                    print(f"  {filename}: MISSING")
                    continue
                target_table = next((table for table in mapping if table_exists(cur, settings.db_schema, table)), None)
                if not target_table:
                    print(f"  {filename}: target table does not exist")
                    continue

                plan = plan_table(
                    cur, s3, settings, filename, target_table, source_path,
                    datetime.now(timezone.utc).isoformat(sep=" "), preview_dir,
                    plans_by_table,
                )
                if plan:
                    plans.append(plan)
                    plans_by_table[target_table] = plan

            conn.rollback()

            failed_plans = [plan.target_table for plan in plans if not plan.ok]
            if args.dry_run == "true":
                if failed_plans:
                    print("\nDry run completed with validation failures: " + ", ".join(failed_plans))
                    return 1
                print("\nDry run completed successfully. No database changes were made.")
                return 0

            if not plans:
                print("No valid plans to execute")
                return 0

            failed_tables: List[str] = []
            committed_tables: List[str] = []

            print("\nWriting each table using an independent transaction...")
            for plan in plans:
                if not plan.ok:
                    failed_tables.append(plan.target_table)
                    print(f"  SKIPPED {plan.target_table}: failed planning validation")
                    write_audit(preview_dir, plan.target_table, "SKIPPED_VALIDATION_FAILED", "Planning validation failed")
                    continue

                try:
                    with conn.cursor() as table_cur:
                        updated, inserted = write_table(table_cur, plan, settings.db_schema)
                        conn.commit()
                    committed_tables.append(plan.target_table)
                    print(f"  COMMITTED {plan.target_table}: updated={updated:,}, inserted={inserted:,}")
                    write_audit(preview_dir, plan.target_table, "COMMITTED", f"updated={updated}; inserted={inserted}")
                except Exception as exc:
                    conn.rollback()
                    failed_tables.append(plan.target_table)
                    reason = str(exc).strip()
                    if getattr(exc, "pgcode", None):
                        reason = f"{exc.pgcode}: {reason}"
                    print(f"  FAILED {plan.target_table}: {reason}")
                    write_audit(preview_dir, plan.target_table, "TABLE_FAILED", reason)

            print("\nExecution summary")
            print(f"Committed: {', '.join(committed_tables) or 'none'}")
            print(f"Failed/skipped: {', '.join(failed_tables) or 'none'}")
            return 1 if failed_tables else 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
