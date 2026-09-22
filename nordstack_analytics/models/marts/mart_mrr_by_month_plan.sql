-- Business question #1: MRR by month and plan.
--
-- Deliberately thin: all the actual logic (what "active this month"
-- means, handling the S00034 effective-end-date situation) lives in
-- int_subscriptions__monthly. This is just the group by.
--
-- monthly_price has no currency column of its own anywhere in this
-- dataset (unlike invoices, which does and needed convert_to_eur()) —
-- every subscription's price is implicitly in the same currency, so no
-- conversion is needed here.

select
    month_start,
    plan_name,
    sum(monthly_price) as mrr,
    count(distinct subscription_id) as active_subscriptions
from {{ ref('int_subscriptions__monthly') }}
group by month_start, plan_name
order by month_start, plan_name
