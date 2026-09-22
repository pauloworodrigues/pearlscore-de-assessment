-- Business question #3: churn.
--
-- Two rates, deliberately both kept: subscription churn (simple count-
-- based) and MRR churn (revenue-weighted). They can disagree in a
-- meaningful way -- losing 5 cheap starter subscriptions is a very
-- different problem than losing 1 scale subscription, and a
-- count-only churn rate can't tell those apart.
--
-- NULLIF guards against divide-by-zero for the first month(s), where
-- there's no prior-month base to churn against yet.

select
    month_start,
    active_subscriptions_start,
    churned_subscriptions,
    round(
        churned_subscriptions::numeric / nullif(active_subscriptions_start, 0),
        4
    ) as subscription_churn_rate,
    mrr_start,
    churned_mrr,
    round(
        churned_mrr::numeric / nullif(mrr_start, 0),
        4
    ) as mrr_churn_rate
from {{ ref('int_subscriptions__churn') }}
order by month_start
