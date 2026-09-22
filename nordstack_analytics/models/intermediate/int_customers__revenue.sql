-- Real logic behind customer LTV: per-customer revenue totals, split by
-- invoice status. mart_customer_ltv is just a left join on top of this.
--
-- LTV here means REALIZED revenue (sum of paid invoices), not a
-- predictive/forward-looking LTV. A predictive LTV would need an
-- assumed churn rate or expected remaining lifetime, which nothing in
-- this dataset gives us grounds to estimate honestly. open/failed
-- totals are surfaced separately as pending/at-risk revenue -- real,
-- useful context, but deliberately not folded into "LTV" itself, since
-- that would overstate realized value with money not actually
-- collected.
--
-- Explicit table (not the intermediate default from dbt_project.yml,
-- spelled out anyway per this project's convention of not relying
-- silently on inherited config): read by mart_customer_ltv, and this
-- aggregation is exactly the kind of join+group-by work that shouldn't
-- recompute from scratch if a second mart ever needs the same numbers.
{{ config(materialized='table') }}

with invoices as (

    select * from {{ ref('stg_billing__invoices') }}

),

subscriptions as (

    select * from {{ ref('stg_billing__subscriptions') }}

),

invoice_customer as (

    select
        i.invoice_id,
        i.amount_eur,
        i.status,
        s.subscription_id,
        s.customer_id
    from invoices as i
    -- Inner join deliberately drops two orphan cases here, consistent
    -- with how they're handled everywhere else in this project: I000601
    -- (#13, invoice with no matching subscription) and any invoice
    -- belonging to S00011 (#7, subscription with no matching customer)
    -- -- neither can be attributed to a real customer, so neither can
    -- contribute to a customer-level LTV number. They're not silently
    -- lost: both are already flagged (has_valid_subscription,
    -- has_valid_customer) and tested for at the staging layer.
    inner join subscriptions as s
        on s.subscription_id = i.subscription_id

)

select
    customer_id,
    count(distinct subscription_id) as subscription_count,
    count(*) as invoice_count,
    sum(case when status = 'paid' then amount_eur else 0 end) as total_paid_eur,
    sum(case when status = 'open' then amount_eur else 0 end) as total_open_eur,
    sum(case when status = 'failed' then amount_eur else 0 end) as total_failed_eur
from invoice_customer
group by customer_id
