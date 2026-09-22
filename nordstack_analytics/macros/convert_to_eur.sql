-- Converts an amount in the given currency to EUR.
--
-- Only two currencies appear in this dataset: EUR (pass through) and SEK
-- (2 invoices, docs/DATA_QUALITY.md #10). The SEK rate below is a fixed,
-- illustrative constant (~0.088 EUR per SEK, i.e. ~11.4 SEK/EUR — roughly
-- where the pair has traded in recent years) chosen because this is a
-- static take-home dataset with no live FX feed to call. A real pipeline
-- would NOT hardcode a rate here: it would join each invoice to a daily
-- FX rate table (or a real-time rate API) keyed by invoice_date and
-- currency, since a fixed constant drifts wrong the moment the real rate
-- moves and silently misstates revenue for every SEK invoice going
-- forward — fine for two known historical rows in an assessment, not
-- fine for a production billing pipeline.
--
-- An unrecognized currency deliberately converts to NULL rather than
-- guessing — that keeps it visible (excluded from SUM-based revenue
-- rather than silently mis-converted) instead of hiding a real gap.

{% macro convert_to_eur(amount_column, currency_column) %}
    case
        when {{ currency_column }} = 'EUR' then {{ amount_column }}
        when {{ currency_column }} = 'SEK' then round({{ amount_column }} * 0.088, 2)
        else null
    end
{% endmacro %}
