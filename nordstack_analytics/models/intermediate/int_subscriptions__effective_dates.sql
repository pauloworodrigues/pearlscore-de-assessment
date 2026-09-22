-- Resolves each subscription's real "active window," so MRR, LTV, and
-- churn don't each have to reinvent this logic (or worse, get it
-- slightly different in each place).
--
-- The reason this needs its own model rather than just using
-- stg_billing__subscriptions.end_date directly: a cancelled subscription
-- can have a NULL end_date after staging's #8 fix (S00034 is the current
-- real example — see that model's SQL for why nulling replaced an
-- earlier, disproven swap fix). A NULL end_date is correct and honest at
-- the staging layer ("we don't know"), but a mart that treats NULL as
-- "still active" would silently count a genuinely cancelled subscription
-- as generating revenue forever. That's a worse error than staging's, and
-- exactly the kind of silent-inflation bug this project has been trying
-- to avoid throughout.
--
-- Fallback: for a cancelled subscription with no reliable end_date, use
-- its own last real invoice date as the best actual evidence of when
-- billing stopped — not a guess, an observed fact about this specific
-- subscription. Active/paused subscriptions with NULL end_date are left
-- alone; NULL there is legitimately "still ongoing," not missing data.

-- Explicit materialized='table' here, not just inherited from
-- dbt_project.yml's intermediate default: this model is read by every
-- mart (MRR, LTV, churn), so a view would recompute its last-invoice
-- aggregation from scratch on every single mart build. Spelled out at
-- the model itself so the reasoning travels with the model, rather than
-- depending on someone reading the project config to know why this one
-- specifically needs to be a table.
{{ config(materialized='table') }}

with subscriptions as (

    select * from {{ ref('stg_billing__subscriptions') }}

),

last_invoice_per_subscription as (

    select
        subscription_id,
        max(invoice_date) as last_invoice_date
    from {{ ref('stg_billing__invoices') }}
    group by subscription_id

)

select

    s.subscription_id,
    s.customer_id,
    s.plan_name,
    s.monthly_price,
    s.status,
    s.start_date,
    s.end_date,

    coalesce(
        s.end_date,
        case when s.status = 'cancelled' then li.last_invoice_date end
    ) as effective_end_date,

    -- Visible so downstream consumers (and the test on this model) can
    -- tell an observed end_date apart from an inferred one, rather than
    -- treating both the same way silently.
    (s.end_date is null and s.status = 'cancelled') as effective_end_date_is_inferred,

    s.had_negative_price_defect,
    s.has_valid_customer

from subscriptions as s
left join last_invoice_per_subscription as li
    on li.subscription_id = s.subscription_id
