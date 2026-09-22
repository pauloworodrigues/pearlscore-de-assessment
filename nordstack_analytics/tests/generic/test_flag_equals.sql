-- Generic test: asserts a boolean flag column equals a given value on
-- every row. One reusable macro instead of one bespoke test per flag —
-- built for the had_negative_price_defect / has_valid_customer pattern
-- used in stg_billing__subscriptions, but works for any boolean "this
-- should always be true/false" column added later.

{% test flag_equals(model, column_name, value) %}

select *
from {{ model }}
where {{ column_name }} != {{ value | lower }}
   or {{ column_name }} is null

{% endtest %}
