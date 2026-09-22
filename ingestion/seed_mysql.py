"""
Load the three raw CSVs into MySQL — standing up the simulated billing system.

This is the first half of step 0: the brief asks us to sync "from MySQL," but
we're only given CSVs, so the source system has to be created before it can
be synced.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Repo root is one level up from this file (ingestion/seed_mysql.py -> repo/).
# Using __file__ instead of a hardcoded path means this script works no
# matter where the repo is cloned or what directory you run it from.
REPO_ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = REPO_ROOT / "seed_data"

# Matches the billing_user credentials from docker-compose.yml. Reading it
# from an env var (with that same value as the fallback) means the same
# script works locally and in CI without editing code — just set MYSQL_URL
# if the connection details ever differ.
MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "mysql+pymysql://billing_user:billing_password@localhost:3306/billing",
)

# (source CSV, target MySQL table, which column to derive updated_at from)
# The third element matters: the CSVs have no updated_at of their own, so we
# manufacture one from each row's own natural timestamp — this becomes the
# real cursor dlt's incremental sync (later in step 0) keys off.
TABLES = [
    ("raw_customers.csv", "customers", "created_at"),
    ("raw_subscriptions.csv", "subscriptions", "start_date"),
    ("raw_invoices.csv", "invoices", "invoice_date"),
]

# Fallback for any row whose natural timestamp is missing or unparseable —
# which, given the planted defects, will happen (see docs/DATA_QUALITY.md).
# A fixed date rather than "now" keeps re-running this script deterministic:
# the same input always produces the same output.
UPDATED_AT_FALLBACK = pd.Timestamp("2024-01-01")


def load_csv(path: Path) -> pd.DataFrame:
    """
    Read a CSV with ZERO type inference.

    dtype=str forces every column to stay text — pandas won't guess that a
    column "looks numeric" and convert it, which matters because we want
    -99.00 (a planted defect) to arrive exactly as written, not silently
    reformatted.

    keep_default_na=False + na_values=[] stops pandas from turning blank
    cells into NaN. A blank amount (finding #9 in DATA_QUALITY.md) should
    stay a literal empty string all the way into MySQL — NaN is a pandas
    concept, not something MySQL or the CSV actually contains.
    """
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def add_updated_at(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
    """
    Derive updated_at from the given column's timestamp.

    errors='coerce' means an unparseable date (there shouldn't be any per
    profiling, but we don't assume) becomes NaT instead of crashing the
    whole load — .fillna() then swaps any NaT for our deterministic fallback.
    format='mixed' lets pandas handle the dates without us hardcoding one
    exact strptime pattern that might not match every row.
    """
    parsed = pd.to_datetime(df[source_col], errors="coerce", format="mixed")
    df = df.copy()  # avoid mutating the caller's DataFrame in place
    df["updated_at"] = parsed.fillna(UPDATED_AT_FALLBACK)
    return df


def main() -> None:
    engine = create_engine(MYSQL_URL)

    # engine.begin() opens a transaction and commits automatically if the
    # block finishes without error, or rolls back on exception — so a
    # failure partway through never leaves MySQL in a half-loaded state.
    with engine.begin() as conn:
        for filename, table, ts_col in TABLES:
            df = load_csv(SEED_DIR / filename)
            df = add_updated_at(df, ts_col)

            # TRUNCATE (not DELETE) is what makes this script idempotent —
            # re-running it always starts each table from empty, so you get
            # the same result whether this is the first run or the tenth.
            conn.execute(text(f"TRUNCATE TABLE {table}"))

            # if_exists="append": the table already exists (created by
            # mysql_init/01_schema.sql on container boot) — we only ever
            # insert rows into it, never let pandas try to create/alter the
            # table's structure itself.
            df.to_sql(table, conn, if_exists="append", index=False)

            print(f"  {filename:>26}  ->  billing.{table:<15} {len(df):>5} rows")

    print("\nMySQL billing database seeded.")


if __name__ == "__main__":
    main()
