-- Singular test encoding a real business rule: a subscription can never
-- end before it starts. This runs against the CLEANED staging output,
-- after the #8 fix — so unlike the raw/staging tests elsewhere in this
-- project, this one is error severity on purpose. It isn't here to prove
-- we caught a planted defect (that's what stg_billing__subscriptions'
-- own nulling logic does); it's here to guarantee our fix actually
-- holds. If this ever fails, the bug is in our transformation logic,
-- not in the source data.

select *
from {{ ref('stg_billing__subscriptions') }}
where end_date is not null
  and end_date < start_date
