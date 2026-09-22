-- Explodes each subscription into one row per calendar month it was
-- active, carrying plan_name and monthly_price for that month. This is
-- the real logic behind "MRR by month/plan" — the mart itself is just a
-- group by on top of this.
--
-- "Active in month M" means: the subscription had started by the end of
-- M, and either never ended or ended during/after M. Uses
-- effective_end_date (int_subscriptions__effective_dates), not raw
-- end_date, specifically so a cancelled subscription with an unreliable
-- end_date (S00034 -- see that model) doesn't either drop out of MRR
-- entirely or, worse, count as active forever.
--
-- Deliberately NOT using the subscriptions_snapshot here: this mart
-- reflects "MRR as computable from data known today," which is what the
-- brief asks for. Point-in-time correctness via the snapshot (what plan
-- was active on a PAST run, before this month's changes) is a real but
-- separate capability -- noted as a possible future direction rather
-- than built now, since nothing in this dataset actually has a plan or
-- price change to demonstrate it against yet (see the snapshot's own
-- comment).

-- Explicit materialized='table' here, not just inherited from
-- dbt_project.yml's intermediate default: this model is read by
-- mart_mrr_by_month_plan today and will be read by the LTV/churn marts
-- too, so a view would recompute the date-spine join from scratch on
-- every single mart build that references it.
{{ config(materialized='table') }}

with subscriptions as (

    select * from {{ ref('int_subscriptions__effective_dates') }}

),

month_spine as (

    select generate_series(
        (select date_trunc('month', min(start_date)) from subscriptions),
        date_trunc('month', current_date),
        interval '1 month'
    )::date as month_start

)

select

    s.subscription_id,
    s.customer_id,
    s.plan_name,
    s.monthly_price,
    m.month_start,
    (m.month_start + interval '1 month')::date as month_end_exclusive

from subscriptions as s
inner join month_spine as m
    -- started by the end of this month...
    on s.start_date < (m.month_start + interval '1 month')::date
    -- ...and either never ended, or ended during/after this month.
    and (s.effective_end_date is null or s.effective_end_date >= m.month_start)
