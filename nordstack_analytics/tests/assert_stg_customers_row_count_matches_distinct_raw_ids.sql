-- Singular test: staging's row count should equal the number of DISTINCT
-- customer_ids in raw, not just "raw row count minus the one duplicate we
-- know about today." That's a real business invariant ("one row per
-- customer"), not a magic-number check tied to today's specific data —
-- it keeps catching the right thing even if more duplicates get planted
-- or introduced later, without us having to update an expected count by
-- hand. A dbt test fails when its query returns any rows, so this
-- returns a row only on mismatch.

select
    (select count(distinct customer_id) from {{ source('billing', 'customers') }}) as expected_customers,
    (select count(*) from {{ ref('stg_billing__customers') }}) as actual_customers
where
    (select count(distinct customer_id) from {{ source('billing', 'customers') }})
    != (select count(*) from {{ ref('stg_billing__customers') }})
