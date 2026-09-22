-- Staging model for the customers source table.
--
-- Materialized as a table rather than the project's staging default
-- (view) — customers stays small even at real-world scale (grows one
-- row per signup, not per billing cycle like invoices), so this isn't
-- about size. It's a deliberate isolation/read-performance choice: other
-- models read a physically materialized table instead of re-running this
-- query's dedup logic on every reference.
--   Pro: faster, isolated reads for anything downstream that joins to
--        customers; the dedup/cleaning work happens once per dbt run,
--        not once per downstream reference.
--   Con: this does NOT give us change history, despite sounding more
--        "permanent" than a view. A table materialization is still a
--        full drop-and-rebuild every run — if a customer's country
--        changes between runs, the old value is gone just as completely
--        as it would be with a view. Real history requires a dbt
--        snapshot (see snapshots/subscriptions_snapshot.sql for where
--        we did use one, and why customers didn't get one too).
--
-- Handles two of the three planted customer-level defects from
-- docs/DATA_QUALITY.md here:
--   #1  exact duplicate row (C0023)      -> deduped below
--   #2  blank country                    -> left as NULL, not a sentinel
-- The third (#3, future created_at on C0041) is deliberately NOT fixed
-- here — it's passed through as-is so the not_future_date test on this
-- model catches and reports it, per the brief's requirement that
-- raw/staging tests catch the planted issues.

{{ config(materialized='table') }}

with source as (

    select
        _row_id,
        customer_id,
        customer_name,
        email,
        -- Blank strings from the CSV land as '' after loading, not NULL —
        -- normalize here so downstream NULL-based logic (COALESCE,
        -- IS NULL checks, aggregations) behaves as expected instead of
        -- silently treating '' as a truthy, non-null value.
        nullif(country, '') as country,
        created_at::date as created_at

    from {{ source('billing', 'customers') }}

),

deduped as (

    select
        *,
        -- C0023 appears twice as an identical row. Partitioning by the
        -- business key and keeping the lowest _row_id is safe here
        -- specifically because we've confirmed (docs/DATA_QUALITY.md)
        -- that every duplicate found so far is a true full-row repeat,
        -- not two different customers who happen to share an id — if
        -- that assumption is ever wrong, the row_count_matches_raw test
        -- in this model's schema.yml is what would catch it.
        row_number() over (
            partition by customer_id
            order by _row_id
        ) as row_num

    from source

)

select
    customer_id,
    customer_name,
    email,
    country,
    created_at
from deduped
where row_num = 1
