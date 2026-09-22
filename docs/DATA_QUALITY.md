# Data quality findings

Findings from a manual profiling pass over the three raw CSVs, done **before**
any modelling, per the brief's warning that the data contains deliberately
planted quality issues. This is the reference list; staging models and their
tests are built against it in the next steps.

## Method

Each file was checked for: nulls/blanks in populated columns, duplicate
business keys and duplicate rows, referential orphans against the other
tables, values outside an accepted set, non-numeric/negative/zero values in
numeric columns, unparseable or impossible dates, and casing/whitespace
inconsistencies in categorical columns.

## customers.csv (121 rows, 120 unique)

| # | Issue | Detail |
|---|---|---|
| 1 | Exact duplicate row | `C0023` appears twice — identical name, email, country, created_at. Not just a duplicate ID; the whole row repeats. |
| 2 | Blank `country` | 1 row. |
| 3 | Future `created_at` | `C0041` is dated 2027-03-15. |

## subscriptions.csv (175 rows, 174 unique)

| # | Issue | Detail |
|---|---|---|
| 4 | Exact duplicate row | `S00006` appears twice, same pattern as C0023. |
| 5 | Status casing | One value is `ACTIVE` instead of `active`. |
| 6 | Negative price | `S00048` (`starter` plan) has `monthly_price = -99.00`. Every other `starter` row is `29.0`; this is a single injected value, not a real tier. |
| 7 | Orphan `customer_id` | `S00011` references `C9999`, which does not exist in customers.csv. |
| 8 | `end_date` before `start_date` | `S00034`: end 2025-03-19, start 2025-03-29. Initially assumed to be a digit transposition (fixable by swapping the two values), but the subscription's 17 real monthly invoices (2025-03-29 through 2026-07-28) rule that out — swapping would just relocate the same broken value. Handled in staging by nulling end_date instead; see `stg_billing__subscriptions.sql` for the full reasoning. |

Blank `end_date` (123 rows) is **not** a defect — verified every one belongs to
a subscription that is `active` or `paused`, never `cancelled`.

## invoices.csv (2,855 rows, no duplicate invoice_id)

| # | Issue | Detail |
|---|---|---|
| 9 | Blank `amount` | 1 row (`I000322`). |
| 10 | Non-EUR currency | 2 rows in `SEK` (`I000101`, `I000201`) — the real FX case, not hypothetical. |
| 11 | Status casing + whitespace | One value is literally `"PAID "` — right word, wrong case, trailing space. Invisible to a naive `distinct()` scan. |
| 12 | Negative amount | 7 rows, all tied to `S00048` — **cascades from finding #6**, not an independent defect. |
| 13 | Orphan `subscription_id` | `I000601` references `S99999`, which does not exist. |
| 14 | Invoices after subscription end | 17 rows for `S00034` — **cascades from finding #8**; resolved by nulling the corrupted `end_date` in staging rather than swapping it (see #8), verified by a dedicated singular test rather than assumed. |

One subscription (`S00149`) has zero invoices, but its `start_date`
(2026-10-05) is in the future relative to today — a brand-new subscription,
not a defect.

## Notes for staging/testing

- Findings #6/#12 and #8/#14 are each **one root cause surfacing in two
  tables**. The fix belongs at the root (subscriptions), and the invoice-level
  symptom should be expected to clear once that fix lands — a test on
  invoices for this shouldn't be treated as an independent rule.
- 13 genuine defects total; 2 things that looked suspicious (blank end_dates,
  the zero-invoice subscription) checked out as legitimate and are called out
  above so they aren't re-flagged later.
- Handling decision (fix / quarantine / exclude) for each item is made in the
  staging layer, not here — this file is the "what we found," staging is the
  "what we did about it."
