-- Type-2 SCD history of subscriptions: every change to status,
-- plan_name, monthly_price, or dates gets a new tracked version instead
-- of overwriting the old one, via dbt_valid_from/dbt_valid_to columns
-- dbt adds automatically.
--
-- Why subscriptions and not customers (see stg_billing__customers.sql
-- for that side of the decision): this has a direct, real correctness
-- consequence for the marts we're about to build. MRR is a BY-MONTH
-- metric — if a subscription's plan or price ever changes (an upgrade,
-- a discount), a model built only from current-state staging data would
-- retroactively apply today's plan/price to every past month, silently
-- rewriting history. Snapshotting means a future MRR-by-month mart can
-- ask "what was this subscription's plan/price as of month X" instead
-- of only ever knowing "what is it now."
--   Pro: correct historical MRR even after plan/price changes; a real,
--        demonstrable need, not history for its own sake.
--   Con: extra storage (one row per change, not per subscription) and
--        one more object to run (dbt snapshot, separate from dbt build)
--        in the orchestration DAG. For THIS dataset specifically, every
--        subscription we've seen keeps one plan/price for its whole
--        life, so this snapshot won't show any real history yet — it's
--        built for correctness under future change, not because today's
--        data needs it.
--
-- Snapshots stg_billing__subscriptions (the CLEANED output), not the raw
-- source — so the tracked history reflects already-corrected data (sign-
-- flipped prices fixed, duplicates removed) rather than replaying the
-- planted defects into permanent history.
--
-- strategy='timestamp' + updated_at is cheaper than strategy='check',
-- which would have to diff every tracked column on every run instead of
-- just comparing one timestamp.

{% snapshot subscriptions_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

-- updated_at is already cast to timestamptz in stg_billing__subscriptions
-- itself (see that model) — needed there rather than here, since dbt's
-- snapshot type check reads this relation's actual declared column type,
-- not how a query here might cast it inline.
--
-- Known cosmetic warning: `dbt snapshot` logs "Data type of snapshot
-- table timestamp columns (DATETIME) doesn't match derived column
-- 'updated_at' (DATETIMETZ)" on every run despite this. Verified via
-- information_schema.columns that every relevant column on both sides —
-- this model's updated_at, the snapshot table's updated_at, and its
-- dbt_valid_from/dbt_valid_to/dbt_updated_at metadata columns — is
-- consistently timestamp with time zone. This is dbt's own type-check
-- macro producing a false positive, not an actual mismatch; snapshotting
-- still runs correctly (confirmed: first run inserted 174 rows, a
-- no-change re-run correctly inserted 0).
select *
from {{ ref('stg_billing__subscriptions') }}

{% endsnapshot %}
