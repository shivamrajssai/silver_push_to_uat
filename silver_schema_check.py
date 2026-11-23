"""Check only this: does each parquet in S3 match its DB table (columns + types)?  Read-only.

Usage:  python check_schema.py N/02015/03002/GJ 1 1 [--s3-root s3://bucket/prefix] [--loose]
  --loose : find the parquet by upc only (ignore cycle / phase in the S3 path)
Env / .env next to this file: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA, S3_SILVER_ROOT
"""
import argparse, io, os, re
from pathlib import Path

import boto3, psycopg2, pyarrow as pa, pyarrow.parquet as pq

S3_SILVER_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver/"      # <-- set to your silver prefix (or --s3-root)
# S3 parquet name -> DB table (None = no DB table)
TABLE_MAP = {
    "geometry_child": "geometry_child",
    "geometry_parent": "geometry_parent",
    "master_chainage": None,
    "master_cracking_10m": "master_cracking_child_10m",
    "master_cracking_as_per_ca": "master_cracking_as_per_ca",
    "master_pothole": "master_pothole",
    "master_ravelling_10m": "master_ravelling_10m",
    "master_ravelling": "master_ravelling",
    "master_roughness_as_per_ca": "master_roughness_as_per_ca",
    "master_roughness_child_10m": "master_roughness_child_10m",
    "master_roughness_parent": "master_roughness_parent",
    "master_rutting_as_per_ca": "master_rutting_as_per_ca",
    "master_rutting_child_10m": "master_rutting_child_10m",
    "master_rutting_parent": "master_rutting_parent",
}


def norm(s):
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def has_upc(rel, upc):
    return f"_{norm(upc)}_" in f"_{norm(rel)}_"


def in_path(rel, upc, cycle, phase, loose):
    p = f"_{norm(rel)}_"
    if not has_upc(rel, upc):
        return False
    return loose or bool(re.search(rf"_(?:cycle|c)_?0*{cycle}_", p) and re.search(rf"_(?:phase|p)_?0*{phase}_", p))


def arrow_cat(t):
    for test, cat in ((pa.types.is_boolean, "bool"), (pa.types.is_integer, "int"), (pa.types.is_floating, "num"),
                      (pa.types.is_decimal, "num"), (pa.types.is_timestamp, "time"), (pa.types.is_date, "time"),
                      (pa.types.is_binary, "bin"), (pa.types.is_large_binary, "bin"), (pa.types.is_list, "list")):
        if test(t):
            return cat
    return "text"


def pg_cat(t):
    t = t.lower()
    if t.endswith("[]"): return "list"
    if t in ("smallint", "integer", "bigint"): return "int"
    if t.startswith(("numeric", "double", "real")): return "num"
    if t == "boolean": return "bool"
    if t.startswith(("timestamp", "date")): return "time"
    if t == "bytea" or t.startswith(("geometry", "geography")): return "bin"
    if t.startswith("json"): return "json"
    return "text"


def compatible(a, p):
    return a == p or p == "json" or (a == "int" and p == "num") or (a == "bin" and p == "text")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("upc"); ap.add_argument("cycle", type=int); ap.add_argument("phase", type=int)
    ap.add_argument("--s3-root"); ap.add_argument("--loose", action="store_true")
    a = ap.parse_args()
    env = Path(__file__).resolve().parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("\"'"))
    schema = os.environ.get("DB_SCHEMA", "public")
    root = a.s3_root or os.environ.get("S3_SILVER_ROOT") or S3_SILVER_ROOT
    bucket, _, prefix = root[5:].partition("/")
    prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""

    conn = psycopg2.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
                            dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
                            password=os.environ["DB_PASSWORD"], connect_timeout=15)
    conn.set_session(readonly=True)
    cur = conn.cursor()
    s3 = boto3.client("s3")

    files, scanned, sample, upc_keys = {}, 0, [], []      # newest parquet per S3 table name
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            scanned += 1
            key, rel = o["Key"], o["Key"][len(prefix):]
            if len(sample) < 15:
                sample.append(key)
            if has_upc(rel, a.upc) and key.lower().endswith(".parquet") and len(upc_keys) < 40:
                upc_keys.append(key)
            t = key.rsplit("/", 1)[-1][:-8].lower()
            if key.lower().endswith(".parquet") and t in TABLE_MAP and in_path(rel, a.upc, a.cycle, a.phase, a.loose):
                if t not in files or o["LastModified"] > files[t][1]:
                    files[t] = (key, o["LastModified"])
    print(f"S3 root s3://{bucket}/{prefix}  ->  {scanned:,} objects scanned, {len(files)} of {len(TABLE_MAP)} parquets matched")
    if not files:
        print("!! Nothing matched. Possible reasons: wrong S3 root (use --s3-root), or cycle/phase not in the path (try --loose).")
        print("Parquet files whose path contains the upc:" if upc_keys else "No parquet path contains the upc. First keys under the root:")
        for k in (upc_keys or sample)[:40]:
            print(f"   {k}")
        return

    summary = []
    for s3_t, db_t in TABLE_MAP.items():
        print(f"\n=== {s3_t}.parquet  ->  DB: {db_t or '(no DB table)'}")
        if db_t is None:
            print("  skipped: no DB table for this parquet")
            summary.append((s3_t, "-", "no DB table", "", ""))
            continue
        cur.execute("""SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull AND NOT a.atthasdef
                       FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname=%s AND c.relname=%s AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped
                       ORDER BY a.attnum""", (schema, db_t))
        db = {n.lower(): (n, ty, req) for n, ty, req in cur.fetchall()}
        if not db or s3_t not in files:
            why = " | ".join(x for x in (f"table {db_t} MISSING in DB" if not db else "",
                                         "parquet NOT FOUND in S3" if s3_t not in files else "") if x)
            print(f"  skipped: {why}")
            summary.append((s3_t, db_t, why, "", ""))
            continue
        print(f"  file: {files[s3_t][0]}")
        sch = pq.read_schema(io.BytesIO(s3.get_object(Bucket=bucket, Key=files[s3_t][0])["Body"].read()))
        pq_cols = {f.name.lower(): (f.name, arrow_cat(f.type), str(f.type)) for f in sch}
        common = [c for c in pq_cols if c in db]
        only_pq = [pq_cols[c][0] for c in pq_cols if c not in db]
        only_db = [db[c][0] for c in db if c not in pq_cols]
        req = [db[c][0] for c in db if c not in pq_cols and db[c][2]]
        bad = [(pq_cols[c][0], pq_cols[c][2], db[c][1]) for c in common if not compatible(pq_cols[c][1], pg_cat(db[c][1]))]
        pct = 100 * len(common) / max(len(db), 1)
        print(f"  columns: parquet {len(pq_cols)}, DB {len(db)}, matching {len(common)}  ->  {pct:.0f}% of DB columns covered")
        print(f"  parquet-only (not in DB): {only_pq or 'none'}")
        print(f"  DB-only (not in parquet): {only_db or 'none'}")
        if req: print(f"  !! DB-only and NOT NULL without default (inserts would fail): {req}")
        print(f"  type differences: {len(bad)}")
        for n, at, pt in bad:
            print(f"    {n}: parquet {at}  vs  DB {pt}")
        ok = not (only_pq or only_db or bad)
        summary.append((s3_t, db_t, "EXACT MATCH" if ok else "DIFFERENCES", f"{len(common)}/{len(db)} ({pct:.0f}%)",
                        f"{len(only_pq)} pq-only, {len(only_db)} db-only, {len(bad)} type"))

    print(f"\n{'=' * 120}\n{'S3 parquet':<30}{'DB table':<30}{'result':<30}{'matched/DB cols':<18}details")
    for r in summary:
        print(f"{r[0]:<30}{r[1]:<30}{r[2]:<30}{r[3]:<18}{r[4]}")
    conn.close()


if __name__ == "__main__":
    main()
