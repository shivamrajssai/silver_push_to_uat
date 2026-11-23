# Silver Parquet to PostgreSQL

This repository contains one consolidated command-line utility for pushing DHMS Silver Parquet datasets from AWS S3 or a local directory into PostgreSQL.

Credentials are never hard-coded in source code. Copy `.env.example` to `.env`, or provide the variables through the environment or deployment secrets.

## Required configuration

| Variable | Required | Purpose |
|---|---:|---|
| `DB_HOST` | Yes | PostgreSQL hostname |
| `DB_PORT` | No | PostgreSQL port; defaults to `5432` |
| `DB_NAME` | Yes | Target database name |
| `DB_USER` | Yes | Database user |
| `DB_PASSWORD` | Yes | Database password |
| `DB_SCHEMA` | No | Target schema; defaults to `public` |
| `S3_SILVER_ROOT` | No | S3 Silver root prefix |
| `AWS_REGION` | No | AWS region; defaults to `ap-south-1` |
| `IRC_FILTER_COLUMNS` | No | Optional comma-separated text columns to scan for 'As per IRC' |

AWS credentials use the standard boto3 credential chain. A long-lived AWS access key should not be stored in the repository.

## Install

```bash
python -m pip install -r requirements.txt
cp .env.example .env
```

Update only the database values in `.env` when targeting a different PostgreSQL database.

## Source file and target-table policy

| # | File name | Action |
|---:|---|---|
| 1 | `geometry_child.parquet` | Push to `geometry_child` |
| 2 | `geometry_parent.parquet` | Push to `geometry_parent` |
| 3 | `master_chainage.parquet` | Do not push |
| 4 | `master_cracking_1000m_lane.parquet` | Do not push |
| 5 | `master_cracking_10m.parquet` | Push to `master_cracking_10m`, with `master_cracking_child_10m` as fallback |
| 6 | `master_cracking_as_per_ca.parquet` | Push to `master_cracking_as_per_ca` |
| 7 | `master_pothole.parquet` | Push to `master_pothole` |
| 8 | `master_ravelling_1000m_lane.parquet` | Do not push |
| 9 | `master_ravelling_10m.parquet` | Push to `master_ravelling_10m` |
| 10 | `master_ravelling.parquet` | Push to `master_ravelling` |
| 11 | `master_roughness_1000m_lane.parquet` | Do not push |
| 12 | `master_roughness_as_per_ca.parquet` | Push to `master_roughness_as_per_ca` |
| 13 | `master_roughness_child_10m.parquet` | Push to `master_roughness_child_10m` |
| 14 | `master_roughness_parent.parquet` | Push to `master_roughness_parent` |
| 15 | `master_rutting_1000m_lane.parquet` | Do not push |
| 16 | `master_rutting_as_per_ca.parquet` | Push to `master_rutting_as_per_ca` |
| 17 | `master_rutting_child_10m.parquet` | Push to `master_rutting_child_10m` |
| 18 | `master_rutting_parent.parquet` | Push to `master_rutting_parent` |

## Data filtering

Before planning or writing, the script scans text columns for `As per IRC` using a case-insensitive pattern. The scan can be restricted with `IRC_FILTER_COLUMNS`. Matching source rows are not pushed and are exported to:

```text
dry_run_output/<target_table>_as_per_irc_excluded.csv
```

This behavior applies before insert/update planning. Missing foreign-key references are validation failures and are never silently converted to `NULL`.

## Dry run

```bash
python silver_parquet_to_uat_push.py \
  --upc "N/01002/01001/GJ" \
  --cycle 1 \
  --phase 1 \
  --dry-run true
```

A dry run discovers source files, matches columns, validates types and foreign keys, and performs no writes.

## Live push

```bash
python silver_parquet_to_uat_push.py \
  --upc "N/01002/01001/GJ" \
  --cycle 1 \
  --phase 1 \
  --dry-run false
```

Each table is planned and written independently:

- one database transaction is used per table;
- a successful table is committed immediately;
- a failure is rolled back only for that table;
- PostgreSQL error code and exact error reason are printed;
- processing continues with the remaining tables;
- the command exits non-zero when one or more tables fail or are skipped.

## Local Parquet input

```bash
python silver_parquet_to_uat_push.py \
  --local-dir /path/to/dlm_kpi_new \
  --dry-run true
```

## Schema-only check

```bash
python silver_parquet_to_uat_push.py \
  --upc "N/02015/03002/GJ" \
  --cycle 1 \
  --phase 1 \
  --check-schema
```

The schema check is read-only and reports missing, extra, and unmatched columns.

## Input alternatives

```bash
python silver_parquet_to_uat_push.py --input "N/01002/01001/GJ 1 1"
python silver_parquet_to_uat_push.py --input-file survey.txt
python silver_parquet_to_uat_push.py --only master_pothole --dry-run true
```

## Targeting three databases

Do not change the Python source for each environment. Change `.env` values for `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD`, or supply environment variables from deployment secrets.

## Legacy files

The original eight Python scripts are retained under `deprecated/` as inert deprecation stubs. They must not be used for migrations. One or more legacy files previously contained an environment-specific credential; rotate any credential that was committed and remove it from Git history before treating the repository as secure.
