-- Business question #2: customer LTV.
--
-- LTV = realized revenue (paid invoices only) -- see
-- int_customers__revenue for the definition and why. Left join from the
-- full customer list, not the revenue aggregate, so a customer with zero
-- billing activity still appears with ltv = 0 rather than being absent
-- from the mart entirely. plan_mix and current_subscription_status come
-- from int_customers__subscription_summary, a separate left join, since
-- that model's scope (all subscriptions) is wider than revenue's
-- (subscriptions with invoice activity) -- see that model's own
-- comment.

select
    c.customer_id,
    c.customer_name,
    c.country,
    ss.plan_mix,
    ss.current_subscription_status,
    coalesce(r.subscription_count, 0) as subscription_count,
    coalesce(r.total_paid_eur, 0) as ltv,
    coalesce(r.total_open_eur, 0) as pending_revenue_eur,
    coalesce(r.total_failed_eur, 0) as failed_revenue_eur
from {{ ref('stg_billing__customers') }} as c
left join {{ ref('int_customers__revenue') }} as r
    on r.customer_id = c.customer_id
left join {{ ref('int_customers__subscription_summary') }} as ss
    on ss.customer_id = c.customer_id
order by ltv desc
