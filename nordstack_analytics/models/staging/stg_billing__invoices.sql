-- Staging model for the invoices source table.
--
-- Materialized as incremental — the real-world-scale argument, distinct
-- from customers (table, isolation) and subscriptions (snapshot,
-- history): invoices is the one table here with genuinely unbounded
-- growth in production (one row per billing cycle, per active
-- subscription, forever). Recomputing a full historical scan on every
-- 5-minute Airflow run, against a table that could be millions of rows
-- after a few years, is the actual problem incremental materialization
-- solves -- not "views are slow" in general (see the discussion this
-- came from), but "re-scanning unbounded history on every run doesn't
-- scale." unique_key=invoice_id with delete+insert means an update to an
-- already-loaded invoice (e.g. status flips open -> paid) replaces that
-- row instead of duplicating it.
--
-- Known inherited limitation: this filters on updated_at, the same
-- synthetic cursor column documented in docs/INGESTION.md as being
-- derived from invoice_date rather than being a true "last written"
-- timestamp. That means a change to an invoice that doesn't also bump
-- updated_at (our seed data never actually does this, but a real status
-- change in production well might not either, if updated_at is sourced
-- the same synthetic way) could be missed by this filter -- the exact
-- same root-cause limitation that poisoned dlt's incremental cursor in
-- step 0, now inherited by this layer too, because it filters on the
-- same synthetic column. Documented rather than hidden.
--
-- Handles all planted invoice-level defects from docs/DATA_QUALITY.md:
--   #9  blank amount (I000322)         -> left NULL, flagged (see below)
--   #10 non-EUR currency (2 SEK rows)  -> converted via convert_to_eur()
--   #11 status casing + whitespace     -> lowercased + trimmed
--   #12 negative amount (7 rows)       -> fixed via abs(), same pattern
--       (cascades from #6)                as the subscriptions price fix
--   #13 orphan subscription_id         -> kept, flagged (see below)
--       (I000601)
--   #14 invoiced after subscription    -> not handled here at all: this
--       end (17 rows, cascades          was a symptom of #8, which is
--       from #8)                        already fixed upstream in
--                                        stg_billing__subscriptions.
--                                        Verified, not just assumed, by
--                                        assert_no_invoices_after_subscription_end.sql

{{
    config(
        materialized='incremental',
        unique_key='invoice_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
    )
}}

with source as (

    select
        _row_id,
        invoice_id,
        subscription_id,
        invoice_date::date as invoice_date,
        nullif(amount, '')::numeric as amount_raw,
        currency,
        lower(trim(status)) as status,
        updated_at::timestamptz as updated_at

    from {{ source('billing', 'invoices') }}

    {% if is_incremental() %}
    -- Only reprocess rows changed since the last run, per the reasoning
    -- above -- not the full historical table every time.
    where updated_at::timestamptz > (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        invoice_id,
        subscription_id,
        invoice_date,
        currency,
        status,
        updated_at,

        -- #12: same sign-flip reasoning as monthly_price in
        -- stg_billing__subscriptions -- abs() corrects it, the flag
        -- below keeps the correction visible instead of silent.
        abs(amount_raw) as amount,

        -- #9: no value to recover here (unlike #12, there's no sign to
        -- flip or inferable magnitude) -- left NULL on purpose rather
        -- than guessed. A NULL amount is correctly excluded from any
        -- SUM(amount)-based revenue total downstream, which is the
        -- honest outcome: we don't know this invoice's amount, so it
        -- shouldn't silently contribute a fabricated number to MRR.
        (amount_raw is null) as had_missing_amount_defect,

        -- coalesce to false when amount is missing: "was it negative"
        -- doesn't apply to a value we don't have, and that case is
        -- already covered by had_missing_amount_defect above -- without
        -- this, the comparison itself would be NULL for I000322 and
        -- trip the flag_equals test for the wrong reason.
        coalesce(amount_raw < 0, false) as had_negative_amount_defect,

        -- #13: kept, not dropped -- same reasoning as the orphan
        -- customer case in stg_billing__subscriptions (a real invoice is
        -- real revenue even if we can't trace it to a live subscription).
        exists (
            select 1
            from {{ source('billing', 'subscriptions') }} as s
            where s.subscription_id = source.subscription_id
        ) as has_valid_subscription

    from source

)

select
    invoice_id,
    subscription_id,
    invoice_date,
    amount,
    currency,
    {{ convert_to_eur('amount', 'currency') }} as amount_eur,
    status,
    had_missing_amount_defect,
    had_negative_amount_defect,
    has_valid_subscription,
    updated_at
from cleaned
