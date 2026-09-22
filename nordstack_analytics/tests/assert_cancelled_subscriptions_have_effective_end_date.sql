-- Singular test, warn severity: flags any cancelled subscription that
-- still has no effective_end_date after the last-invoice fallback in
-- int_subscriptions__effective_dates — i.e. a cancelled subscription
-- with no end_date AND no invoices at all. Not a build-breaking error
-- (there's nothing more we can infer from this data), but genuinely
-- worth knowing about rather than silently excluding these subscriptions
-- from churn/MRR calculations without a trace.

{{ config(severity='warn') }}

select *
from {{ ref('int_subscriptions__effective_dates') }}
where status = 'cancelled'
  and effective_end_date is null
