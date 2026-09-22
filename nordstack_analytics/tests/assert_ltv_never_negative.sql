-- Singular test encoding a real business rule: LTV (realized revenue)
-- can never be negative. Error severity — this is a sanity check on the
-- mart's own aggregation, not a known planted defect (those are already
-- fixed upstream in staging).

select *
from {{ ref('mart_customer_ltv') }}
where ltv < 0
