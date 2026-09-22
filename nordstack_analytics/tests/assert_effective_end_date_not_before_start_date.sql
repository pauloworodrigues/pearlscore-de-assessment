-- Singular test, error severity: the inferred effective_end_date (via
-- last-invoice fallback) should never end up before start_date. Unlike
-- the warn-severity tests in this project, this one isn't watching for a
-- known planted defect — it's a sanity check on our OWN derivation logic
-- in int_subscriptions__effective_dates. If this ever fails, the bug is
-- in that model, not in the source data.

select *
from {{ ref('int_subscriptions__effective_dates') }}
where effective_end_date is not null
  and effective_end_date < start_date
