-- Singular test encoding a real business rule: a churn rate is a
-- proportion of an existing base, so it can never be negative and
-- (barring some subscription churning and un-churning within the same
-- reporting window, which this data model doesn't represent) should
-- never exceed 1 (100%). Error severity — sanity check on the mart's
-- own math, not a known planted defect.

select *
from {{ ref('mart_churn') }}
where subscription_churn_rate < 0 or subscription_churn_rate > 1
   or mrr_churn_rate < 0 or mrr_churn_rate > 1
