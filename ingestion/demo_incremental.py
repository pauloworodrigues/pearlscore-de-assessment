"""
Proves the dlt pipeline's incremental behavior end to end.

Run this, then re-run `python ingestion/dlt_pipeline.py` normally (no
FULL_REFRESH) and watch the Extract step: it should report
`subscriptions: 2` instead of scanning all 175 rows. That's the cursor
(tracked in Postgres's _dlt_pipeline_state) doing its job — only rows
with updated_at newer than the last run get pulled.

Two mutations, on purpose: an UPDATE proves existing rows get picked up
and merged in place; an INSERT proves brand-new rows get picked up too.
"""

from datetime import datetime, timezone

from sqlalchemy import create_engine, text

MYSQL_URL = "mysql+pymysql://billing_user:billing_password@localhost:3306/billing"

# A row we know exists (checked against the raw CSV): a simple, unremarkable
# active starter subscription — nothing planted-defect-related about it, so
# the before/after is easy to read.
EXISTING_SUBSCRIPTION_ID = "S00001"

# A subscription_id that doesn't exist yet. Deliberately far from S99999,
# which is the orphan id already planted as one of the 13 defects — we don't
# want this demo row confused with that one.
NEW_SUBSCRIPTION_ID = "S00998"
NEW_SUBSCRIPTION_CUSTOMER_ID = "C0001"  # exists in raw_customers.csv

# Deliberately NOT one of the three real plans (starter/growth/scale) --
# this row only ever needs to exist in MySQL/raw Postgres to prove the
# incremental cursor picks it up, never in dbt. If you run this demo and
# THEN run a full `dbt build`, mart_mrr_by_month_plan's accepted_values
# test on plan_name will (correctly) flag this row -- that's expected,
# not a bug, since this script's data was never meant to flow through
# the dbt project. Truncate/reseed MySQL (seed_mysql.py) before a real
# dbt build if you've run this demo.


def main() -> None:
    engine = create_engine(MYSQL_URL)
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # MySQL DATETIME has no tz

    with engine.begin() as conn:
        # --- Mutation 1: update an existing row ---------------------------
        # Cancelling a currently-active subscription is a realistic mutation,
        # and bumping updated_at is what makes dlt's incremental cursor
        # notice this row again on the next run.
        result = conn.execute(
            text(
                """
                UPDATE subscriptions
                SET status = 'cancelled',
                    end_date = :today,
                    updated_at = :now
                WHERE subscription_id = :sub_id
                """
            ),
            {"sub_id": EXISTING_SUBSCRIPTION_ID, "today": now.date().isoformat(), "now": now},
        )
        print(f"Updated {result.rowcount} row(s) for {EXISTING_SUBSCRIPTION_ID}")

        # --- Mutation 2: insert a brand-new row ----------------------------
        # Guarded with a check first so re-running this script twice doesn't
        # insert duplicates — subscription_id isn't a real primary key in
        # this schema (only _row_id is), so nothing else would stop us.
        exists = conn.execute(
            text("SELECT 1 FROM subscriptions WHERE subscription_id = :sub_id"),
            {"sub_id": NEW_SUBSCRIPTION_ID},
        ).first()

        if exists:
            print(f"{NEW_SUBSCRIPTION_ID} already exists — skipping insert (already ran this demo once)")
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO subscriptions
                        (subscription_id, customer_id, plan_name, monthly_price,
                         start_date, end_date, status, updated_at)
                    VALUES
                        (:sub_id, :cust_id, 'pro', '79.0', :today, NULL, 'active', :now)
                    """
                ),
                {
                    "sub_id": NEW_SUBSCRIPTION_ID,
                    "cust_id": NEW_SUBSCRIPTION_CUSTOMER_ID,
                    "today": now.date().isoformat(),
                    "now": now,
                },
            )
            print(f"Inserted new row {NEW_SUBSCRIPTION_ID}")

    print("\nMySQL mutated. Now run: python ingestion/dlt_pipeline.py")
    print("Watch the Extract step — it should show subscriptions: 2, not 175.")


if __name__ == "__main__":
    main()
