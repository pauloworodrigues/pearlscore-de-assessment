-- Singular test encoding a real business rule: an invoice shouldn't be
-- billed after its subscription has ended. This was defect #14
-- (docs/DATA_QUALITY.md) — 17 invoices for S00034 that looked like
-- post-cancellation billing, but were actually a symptom of #8's
-- corrupted end_date, not an independent bug. stg_billing__subscriptions
-- nulls out end_date when it's before start_date rather than swapping
-- it with start_date (a swap was tried first and disproven — see that
-- model's SQL), which is what actually resolves this cascade: with no
-- reliable end_date, there's nothing for these invoices to be "after."
--
-- Error severity is deliberate: nothing in stg_billing__invoices touches
-- this directly, so if this ever returns rows, it means the #8 fix in
-- stg_billing__subscriptions stopped working — a real regression worth
-- failing the build over, not a known/expected defect to warn about.

select i.*
from {{ ref('stg_billing__invoices') }} as i
inner join {{ ref('stg_billing__subscriptions') }} as s
    on i.subscription_id = s.subscription_id
where s.end_date is not null
  and i.invoice_date > s.end_date
