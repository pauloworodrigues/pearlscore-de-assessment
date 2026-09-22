-- Generic (reusable, parameterized) test: flags any row where the given
-- column is a date later than today.
--
-- Directly born from a real finding in step 0 (docs/INGESTION.md): a
-- future-dated column isn't just "weird data," it can silently corrupt
-- dlt's incremental cursor. Apply this ONLY to columns where "in the
-- future" is genuinely always wrong — a customer's created_at, an
-- invoice's invoice_date. Do NOT apply it to something like a
-- subscription's start_date, where a future value can be entirely
-- legitimate (a subscription scheduled to start next month, as we saw
-- with S00149) — that's a business-meaning judgment call per column,
-- not something this generic macro can know on its own.

{% test not_future_date(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} > current_date

{% endtest %}
