-- Staging model for the subscriptions source table.
--
-- Handles all five planted subscription-level defects from
-- docs/DATA_QUALITY.md:
--   #4 exact duplicate row (S00006)   -> deduped, same pattern as customers
--   #5 status casing (ACTIVE)         -> lowercased + trimmed
--   #6 negative price (S00048)        -> fixed via abs(), flagged (see below)
--   #7 orphan customer_id (S00011)    -> kept, flagged (see below)
--   #8 end_date before start_date    -> end_date nulled out (an earlier
--      (S00034)                          swap-based fix was tried and
--                                         disproven by the invoice
--                                         history -- see below)

with source as (

    select
        _row_id,
        subscription_id,
        customer_id,
        plan_name,
        monthly_price::numeric as monthly_price_raw,
        nullif(start_date, '')::date as start_date_raw,
        nullif(end_date, '')::date as end_date_raw,
        lower(trim(status)) as status,
        -- Passed through (unlike the other columns here) specifically so
        -- snapshots/subscriptions_snapshot.sql can use it as a cheap
        -- timestamp-strategy change-detection column, instead of the
        -- heavier check strategy (which has to diff every tracked column
        -- on every run). Cast to timestamptz here, at the source, rather
        -- than only inside the snapshot's query — dbt's snapshot
        -- validity check compares its target table's column type against
        -- this relation's actual declared column type (via database
        -- metadata), not against how a downstream query happens to cast
        -- it inline, so the cast has to live here to actually satisfy it.
        updated_at::timestamptz as updated_at

    from {{ source('billing', 'subscriptions') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by subscription_id
            order by _row_id
        ) as row_num

    from source

)

select

    subscription_id,
    customer_id,
    plan_name,

    -- #6: sign-flip correction via abs(), not a hardcoded "correct" price
    -- per plan. Evidence (every other starter row is 29.0, this is the
    -- only negative value anywhere in the column) points to a data-entry
    -- sign error, not a genuinely different price tier — abs() fixes any
    -- such sign flip generally, without us having to hardcode what "the
    -- right price" is for every plan.
    --   Pro: preserves the row and its real revenue in MRR/LTV instead of
    --        losing it to quarantine over what's very likely a typo.
    --   Con: assumes every future negative price is the same kind of
    --        error; a genuinely different failure mode (e.g. a refund
    --        encoded as negative) would be silently "fixed" the same way.
    --        The had_negative_price_defect flag below is what keeps this
    --        assumption visible and checkable rather than silent.
    abs(monthly_price_raw) as monthly_price,
    (monthly_price_raw < 0) as had_negative_price_defect,

    -- #8: originally "fixed" by swapping start_date/end_date back,
    -- assuming a transposition. That theory didn't survive contact with
    -- the actual invoice history: S00034 has 17 real monthly invoices
    -- running from 2025-03-29 through 2026-07-28, so swapping just moved
    -- the same broken value into end_date instead of fixing anything --
    -- the very next invoice after the "fixed" end_date would still
    -- violate the cascade check. There's no reliable true end_date
    -- recoverable from this data (status = 'cancelled' doesn't match 17
    -- months of ongoing billing either, but that inconsistency is out of
    -- scope here -- we're not asked to fix status).
    --   Pro of nulling instead of guessing: doesn't fabricate a value we
    --        have no evidence for; the singular test on invoices
    --        (assert_no_invoices_after_subscription_end) naturally stops
    --        flagging this row once there's no end_date to compare
    --        against, which is the honest outcome -- we don't actually
    --        know when (or if) this subscription ended.
    --   Con: a subscription's true cancellation date is now unknown
    --        rather than wrong-but-present, which could understate churn
    --        analysis for this one row if a mart naively treats NULL
    --        end_date as "still active" without checking status too.
    -- start_date is left untouched -- nothing in the evidence suggests
    -- it's wrong, only end_date is.
    start_date_raw as start_date,
    case when end_date_raw < start_date_raw then null else end_date_raw end as end_date,

    status,

    -- #7: no dropping here, on purpose.
    --   Pro: a subscription with a missing customer can still represent
    --        real, already-invoiced revenue — dropping it would silently
    --        understate MRR/total revenue over what looks like an
    --        upstream referential bug (e.g. a customer deleted without
    --        cascading), which is a worse failure mode for billing data
    --        than surfacing the gap.
    --   Con: this row can never be attributed to a customer, so
    --        customer-level marts (LTV) naturally can't include it — an
    --        inner join to customers there excludes it correctly without
    --        needing special-case logic.
    -- Flagged (not silently passed through) so it's visible and testable
    -- rather than discoverable only by someone stumbling on a bad join.
    exists (
        select 1
        from {{ source('billing', 'customers') }} as c
        where c.customer_id = deduped.customer_id
    ) as has_valid_customer,

    updated_at

from deduped
where row_num = 1
