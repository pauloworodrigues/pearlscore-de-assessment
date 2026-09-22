-- Second half of "customer LTV" per the brief: plan_mix and
-- current_subscription_status, alongside int_customers__revenue's
-- revenue totals. Kept as its own model rather than folded into
-- int_customers__revenue because the scope is different: revenue only
-- knows about subscriptions with invoice activity (inner joined through
-- invoices there), but plan mix and current status need to reflect ALL
-- of a customer's subscriptions -- including a brand-new one with no
-- invoices yet (S00149) that revenue would never see.
--
-- "Current" subscription = most recent by start_date. A customer can
-- have more than one subscription over time (a customer can upgrade by
-- cancelling one plan and starting another, or run more than one
-- concurrently), so "current status" needs a tiebreak rule rather than
-- assuming there's only ever one row to look at -- most-recent-by-
-- start_date is the natural reading of "current."
--
-- Only considers subscriptions that resolve to a real customer
-- (has_valid_customer) -- same #7 orphan reasoning used everywhere else
-- in this project: S00011 can't contribute to a customer-level summary
-- for a customer it doesn't actually belong to.
{{ config(materialized='table') }}

with subscriptions as (

    select *
    from {{ ref('int_subscriptions__effective_dates') }}
    where has_valid_customer

),

ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by start_date desc, subscription_id desc
        ) as recency_rank
    from subscriptions

)

select
    customer_id,
    string_agg(distinct plan_name, ', ' order by plan_name) as plan_mix,
    -- max() over a column that's NULL for every row except the single
    -- recency_rank = 1 one is a simple way to pick "that row's value"
    -- without a second query back to the ranked CTE.
    max(case when recency_rank = 1 then status end) as current_subscription_status
from ranked
group by customer_id
