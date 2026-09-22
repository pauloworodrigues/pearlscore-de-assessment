"""
STEP 0 — sync the NordStack billing tables from MySQL into Postgres with dlt.

This is the actual "sync" step 0 asks for. seed_mysql.py stood up the source;
this script moves that data into Postgres's `raw` schema, which is what dbt
will read from in the next step.

Why dlt instead of a plain COPY / INSERT SELECT script: a COPY moves data
once. dlt moves data repeatedly and correctly — it tracks what it already
loaded (in tables it creates itself: _dlt_loads, _dlt_pipeline_state), so a
second run only pulls what changed instead of reloading everything.
"""

from __future__ import annotations

import os

import dlt
from dlt.sources.sql_database import sql_database

MYSQL_URL = os.getenv(
    "MYSQL_URL",
    "mysql+pymysql://billing_user:billing_password@localhost:3306/billing",
)

# table -> merge key. ALWAYS the MySQL surrogate (_row_id), never the
# business key (customer_id, subscription_id, invoice_id).
#
# Why this matters: if we merged on customer_id, dlt would treat the two
# planted C0023 rows as "the same record loaded twice" and collapse them
# into one — silently deduping a defect before dbt ever sees it. Keying on
# the meaningless surrogate keeps both rows distinct all the way through,
# same reasoning as the MySQL schema itself (see mysql_init/01_schema.sql).
RESOURCES = {
    "customers": "_row_id",
    "subscriptions": "_row_id",
    "invoices": "_row_id",
}

# The cursor column seed_mysql.py added, since the source CSVs don't have
# one of their own.
CURSOR_COLUMN = "updated_at"


def build_source(full_refresh: bool = False):
    """
    Configure the sql_database source: which tables, and how each one loads.

    reflection_level="minimal" tells dlt not to aggressively infer/tighten
    MySQL's column types on its way through — the raw layer stays as loosely
    typed as the source, consistent with the "don't clean anything before
    dbt sees it" rule we've followed since the schema design.
    """
    source = sql_database(
        credentials=MYSQL_URL,
        reflection_level="minimal",
    ).with_resources(*RESOURCES)

    for table, merge_key in RESOURCES.items():
        hints = {
            "primary_key": merge_key,
            # merge = upsert. On a normal run this makes re-runs idempotent:
            # loading the same row twice updates it in place instead of
            # duplicating it. full_refresh=True (below) overrides this with a
            # full wipe-and-reload instead, for when you want a clean slate.
            "write_disposition": "replace" if full_refresh else "merge",
        }
        if not full_refresh:
            # dlt's incremental cursors are >=, not >, so a row exactly at
            # the last-seen updated_at gets re-fetched on the next run. That
            # would normally risk a duplicate, but write_disposition="merge"
            # makes that re-fetch harmless — it just updates the same row
            # again rather than inserting a copy.
            hints["incremental"] = dlt.sources.incremental(CURSOR_COLUMN)
        source.resources[table].apply_hints(**hints)

    return source


def main() -> None:
    # FULL_REFRESH=1 forces a clean replace load, ignoring any stored cursor
    # state from a previous run. Useful for testing, or for the "reproducible
    # from a clean clone" path when there's no prior state to be incremental
    # against anyway.
    full_refresh = os.getenv("FULL_REFRESH", "").lower() in {"1", "true", "yes"}

    pipeline = dlt.pipeline(
        pipeline_name="nordstack_billing",
        destination="postgres",
        dataset_name="raw",   # -> Postgres schema `raw`. This is the seam
                               # dbt's sources.yml will point at.
        progress="log",
    )

    info = pipeline.run(build_source(full_refresh=full_refresh))
    print(info)

    # Print how many rows actually moved this run — the concrete evidence
    # that a second run is genuinely incremental, not just claiming to be.
    print("\nrows loaded this run:")
    row_counts = pipeline.last_trace.last_normalize_info.row_counts or {}
    for table, count in row_counts.items():
        if not table.startswith("_dlt"):   # skip dlt's own bookkeeping tables
            print(f"  {table:<20} {count:>6}")


if __name__ == "__main__":
    main()
