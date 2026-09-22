-- Real logic behind churn: per month, how many subscriptions (and how
-- much MRR) were active at the start of the month vs. cancelled during
-- it. mart_churn is just the ratio division on top of this.
--
-- "Active at the start of month M" = active during month M-1 (i.e. the
-- base you're measuring loss against is the subscriber base you already
-- had going in, not month M's own activity, which would conflate new
-- signups with retention of existing ones). Reuses
-- int_subscriptions__monthly's month grain directly (a self-join shifted
-- by one month) rather than re-deriving a date spine here.
--
-- "Churned during month M" = subscriptions with status = 'cancelled'
-- whose effective_end_date falls in month M. Deliberately keyed on
-- effective_end_date (int_subscriptions__effective_dates), same
-- reasoning as everywhere else in this project: S00034's raw end_date
-- is unreliable, and effective_end_date is what correctly resolves it
-- (via the last-invoice fallback) instead of either dropping it from
-- churn entirely or leaving it stuck with a nonsensical date.
{{ config(materialized='table') }}

with monthly as (

    select * from {{ ref('int_subscriptions__monthly') }}

),

subscriptions as (

    select * from {{ ref('int_subscriptions__effective_dates') }}

),

months as (

    select distinct month_start from monthly

),

active_at_month_start as (

    -- Shifted by one month: "active at the start of M" is really "active
    -- during M-1," reusing the same active-window logic already computed
    -- in int_subscriptions__monthly rather than redefining it here.
    select
        (m.month_start + interval '1 month')::date as month_start,
        count(distinct mo.subscription_id) as active_subscriptions_start,
        sum(mo.monthly_price) as mrr_start
    from months as m
    inner join monthly as mo
        on mo.month_start = m.month_start
    group by m.month_start

),

churned_during_month as (

    select
        date_trunc('month', effective_end_date)::date as month_start,
        count(distinct subscription_id) as churned_subscriptions,
        sum(monthly_price) as churned_mrr
    from subscriptions
    where status = 'cancelled'
      and effective_end_date is not null
    group by date_trunc('month', effective_end_date)

)

select
    m.month_start,
    coalesce(a.active_subscriptions_start, 0) as active_subscriptions_start,
    coalesce(a.mrr_start, 0) as mrr_start,
    coalesce(c.churned_subscriptions, 0) as churned_subscriptions,
    coalesce(c.churned_mrr, 0) as churned_mrr
from months as m
left join active_at_month_start as a
    on a.month_start = m.month_start
left join churned_during_month as c
    on c.month_start = m.month_start
order by m.month_start
