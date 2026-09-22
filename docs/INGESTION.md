# Step 0: ingestion pipeline

## Overview

`seed_mysql.py` loads the three raw CSVs into MySQL, simulating a live
billing system as the brief describes. `dlt_pipeline.py` then syncs
MySQL -> Postgres (`raw` schema) using
[dlt](https://dlthub.com)'s `sql_database` source, one dlt resource per
table (`customers`, `subscriptions`, `invoices`).

Each resource is configured with:

- `primary_key = _row_id` — the surrogate key MySQL assigns on insert, not
  a business key. Business keys (`customer_id`, `subscription_id`,
  `invoice_id`) aren't reliable merge keys here on their own, since two of
  the planted defects are exact duplicate rows sharing a business key
  (`C0023`, `S00006`).
- `write_disposition = "merge"` — updates to an existing row land as an
  update in Postgres, not a duplicate insert.
- `incremental` on `updated_at` — only rows changed since the last run are
  pulled, tracked automatically by dlt in `_dlt_pipeline_state`.

`demo_incremental.py` exists to prove the incremental behavior isn't just
configured but actually working: it mutates one existing MySQL row and
inserts one new one, then a normal (non-`FULL_REFRESH`) pipeline run should
extract exactly those two rows instead of rescanning the table.

## Known limitation: the `updated_at` cursor can be poisoned by future dates

Running `demo_incremental.py` surfaced a real design flaw, not just a demo
hiccup, so it's documented here rather than quietly worked around.

**What happened.** After mutating one `subscriptions` row and inserting
another (both timestamped with the actual run time), the next
`dlt_pipeline.py` run extracted **zero** rows for `customers`,
`subscriptions`, and `invoices` — including the two rows that had just
changed.

**Why.** dlt's incremental cursor for each resource is "the maximum
`updated_at` value seen so far"; it only pulls rows *after* that value on
the next run. In this project, `updated_at` isn't a real system timestamp —
it doesn't exist in the source CSVs, so `seed_mysql.py` derives it from a
business date column per table (`created_at` for customers, `start_date`
for subscriptions, `invoice_date` for invoices), as a stand-in for "when
this row last changed."

That derivation conflates two different things: a *business* date (when a
subscription starts, when a customer signed up) and a *system* timestamp
(when the row was actually last written). They usually agree, but not
always:

- `customers`: `C0041` has a planted future `created_at` (2027-03-15) —
  one of the 13 documented defects.
- `subscriptions`: `S00149` has a **legitimate**, non-defective future
  `start_date` (2026-10-05) — a subscription scheduled to start next
  month. Documented in `DATA_QUALITY.md` as a false positive during
  profiling, precisely because nothing is wrong with the row.

Either way, once a row like that is loaded, it becomes the cursor's
high-water mark. Every subsequent legitimate update — timestamped with the
*actual* current time, which is earlier than that future value — looks
"older" than what the pipeline has already seen, and is silently skipped.
No error, no warning: the sync just goes quietly stale until real time
catches up to the poisoned watermark.

This is a stronger finding than a failed `dbt test` would be: it shows a
single bad or even just unusually-shaped row can break sync *correctness*
upstream of dbt entirely, before any test has a chance to catch it.

**What a real fix would look like.** In a production source system,
`updated_at` should be a monotonic value the database itself maintains on
write (e.g. a trigger-maintained `updated_at`, or the natural write-time
semantics of a CDC log), never derived from a business-meaningful date a
user or process can set to any value, past or future. Short of that, an
ingestion-side safeguard — clamping or quarantining any `updated_at`
value greater than the current sync time before it can update the cursor
— would prevent a single row from poisoning incremental state for
everything after it.

This project keeps the derivation and the demo's result exactly as they
ran, rather than adjusting the seed data or the demo to avoid the issue,
since it's a more honest and more informative record of what the design
choice actually causes.
