-- Singular test encoding a real business rule: MRR can never be
-- negative — there's no such thing as negative recurring revenue.
-- Error severity: this isn't watching for a known planted defect (those
-- are already fixed upstream in staging), it's a sanity check on the
-- mart's own aggregation logic.

select *
from {{ ref('mart_mrr_by_month_plan') }}
where mrr < 0
