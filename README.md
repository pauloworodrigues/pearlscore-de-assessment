# NordStack billing analytics — take-home submission

A trustworthy analytics layer for NordStack, a fictional B2B SaaS selling
`starter` / `growth` / `scale` subscription plans across Europe, built from
three raw CSV exports of its billing system.

This README covers setup, project structure, the modeling decisions behind
it, the data-quality issues found and how each was handled, and what I'd do
next with more time.

## Setup

Two paths are provided, both reproducible from a clean clone. Path A (dlt)
is what I actually built and ran; Path B is the brief's fallback if you'd
rather skip MySQL and seed Postgres directly.

### Prerequisites

- Docker (for Postgres, and MySQL if using Path A)
- Python 3.11+
- `pip install -r requirements.txt` (installs everything both paths need —
  see the file's comments for which section belongs to which step)

### Path A — dlt pipeline (MySQL -> Postgres)

This is the optional step 0 from the brief, and the path this project
actually uses end to end.

```bash
# 1. Bring up MySQL and Postgres
docker compose up -d

# 2. Load the three CSVs into MySQL, simulating the live billing system
python ingestion/seed_mysql.py

# 3. Sync MySQL -> Postgres (raw schema) with dlt
cp .dlt/example.secrets.toml .dlt/secrets.toml   # fill in your own MySQL URL if it differs
python ingestion/dlt_pipeline.py
```

`ingestion/demo_incremental.py` is a standalone script proving the
incremental sync actually works (and, in the process, surfaces a real
design flaw — see [Known limitation: the `updated_at` cursor can be
poisoned](docs/INGESTION.md)). It's not part of the required setup path;
run it only if you want to see that behavior yourself.

### Path B — seed Postgres directly (skip dlt)

If you'd rather not stand up MySQL at all:

```bash
docker compose up -d postgres
# load seed_data/*.csv into a `raw` schema in Postgres however you prefer —
# COPY, a small script, or dbt seed all work; the dbt project only cares
# that raw.customers / raw.subscriptions / raw.invoices exist with the
# same columns as the CSVs.
```

### dbt

```bash
cd nordstack_analytics
pip install dbt-core dbt-postgres   # already in requirements.txt
mkdir -p ~/.dbt && cp ../profiles.yml.example ~/.dbt/profiles.yml
# edit ~/.dbt/profiles.yml if your Postgres isn't on the default port/creds

dbt deps
dbt snapshot   # Type-2 SCD snapshot of subscriptions — see "Materialization strategy" below for why this is a separate command from dbt build
dbt build --exclude "resource_type:snapshot"   # runs every model + every test; see below for why --exclude is needed here
dbt docs generate && dbt docs serve   # optional, browsable documentation
```

`dbt build` should exit green. Every planted defect that reaches a test is
caught and reported (mostly as `WARN`, by design — see [Data-quality
findings](#data-quality-findings) below), not as a build-breaking `ERROR`.

The `--exclude "resource_type:snapshot"` flag is required, not optional
decoration: dbt-core 1.12 (what `dbt-core==1.*` resolves to as of this
writing) includes snapshots in `dbt build`'s node graph by default —
confirmed by actually running it, not assumed. Without that flag,
`dbt build` silently re-runs the snapshot a second time on top of the
explicit `dbt snapshot` above, which is harmless (snapshotting twice in a
row just finds nothing changed) but defeats the entire point of keeping
snapshot a separate, explicit step.

## Project structure

```
seed_data/                  raw CSVs (input, not touched by any script)
mysql_init/01_schema.sql    MySQL DDL for the simulated source system
docker-compose.yml          Postgres 16 + MySQL 8
ingestion/
  seed_mysql.py             CSVs -> MySQL
  dlt_pipeline.py           MySQL -> Postgres (raw schema), via dlt
  demo_incremental.py       proves incremental sync works (and surfaces its limitation)
docs/
  DATA_QUALITY.md           manual profiling pass — the 13 defects found, before any modeling
  INGESTION.md               step-0 design notes, incl. the cursor-poisoning finding
nordstack_analytics/         the dbt project
  models/
    staging/                 one model per source table: rename, type, clean, flag defects
    intermediate/             reusable business logic, shared by multiple marts
    marts/                    thin, final answers to the three business questions
  snapshots/                  Type-2 SCD history of subscriptions
  macros/                     convert_to_eur(), and the two custom generic tests
  tests/                      singular (custom SQL) tests
airflow/                      DAG running dbt on a schedule (see Orchestration)
```

## Key modeling decisions

### Layering: staging -> intermediate -> marts

- **staging** is a near-1:1 mirror of each raw source table: renamed,
  typed, deduplicated, and cleaned, with every fix left visible via a
  boolean `had_..._defect` / `has_valid_...` flag rather than silently
  disappearing. This is where every "fix, quarantine, or exclude" decision
  from the brief actually gets made and documented — see the comments in
  each `stg_*.sql` file and [Data-quality findings](#data-quality-findings)
  below.
- **intermediate** holds every non-trivial transformation: the date-spine
  explosion behind MRR, the last-invoice fallback behind churn, the
  per-customer revenue and subscription-history aggregations behind LTV.
  Nothing in this layer is meant to be queried by anyone outside the
  project — it exists purely so multiple marts can share one correct
  definition of "active this month" or "this customer's current status"
  instead of each mart reimplementing (and risking subtly disagreeing on)
  the same logic.
- **marts** are deliberately thin — a `select` / `group by` / simple `join`
  on top of intermediate models, nothing more. Each one maps directly to
  one of the brief's three business questions. Keeping the heavy lifting
  out of the marts means the "final answer" layer stays easy to read,
  easy to trust, and easy to verify against raw data by eye.

### Materialization strategy

The brief asks us to explain *why*, not just declare `view` or `table`, so
here's the full reasoning — the same reasoning is also left as a comment
on every model that sets an explicit `config()` block, deliberately, even
where it just repeats the folder-level default in `dbt_project.yml`, so
the reasoning travels with the model rather than depending on someone
finding the project config.

| Layer / model | Materialization | Why |
|---|---|---|
| `staging` (default) | `view` | Cheap to compute, read once per downstream reference, never queried directly outside this project. No reason to pay a storage/refresh cost for it. |
| `stg_billing__customers` | `table` | The one staging exception. Not about size — customers grows one row per signup, tiny even at real-world scale — this is an isolation/read-performance choice: the dedup logic runs once per dbt run instead of once per downstream reference. Explicitly **not** a history mechanism: a table is still a full drop-and-rebuild every run, so a changed `country` value is gone just as completely as it would be with a view. |
| `stg_billing__invoices` | `incremental` | The real-world-scale argument, and the one table here with genuinely unbounded growth in production — one row per billing cycle, forever. Rescanning full history on every 5-minute Airflow run doesn't scale once this is millions of rows, even though the actual seed dataset is tiny. `unique_key=invoice_id` + `delete+insert` means a status change (`open` -> `paid`) replaces the row instead of duplicating it. Carries forward an inherited limitation from the dlt layer — see [Known limitation](docs/INGESTION.md). |
| `subscriptions_snapshot` | `snapshot` (Type-2 SCD) | Subscriptions, not customers, got a real snapshot, because it has a genuine correctness need a plain table can't solve: MRR is a *by-month* metric, so if a subscription's plan or price ever changes, a model built only from current-state data would retroactively apply today's price to every past month. This dataset happens not to have any actual plan/price changes to demonstrate against yet — the snapshot is built for correctness under future change, not because today's data needs it (see [What I'd do next](#what-id-do-next-with-more-time)). |
| `intermediate` (default) | `table` | Every model here is read by more than one mart. A view would recompute its full query — the date-spine join, the last-invoice aggregation — from scratch on every single mart build that references it. Computing once per `dbt run` and reading many times is the entire reason this layer exists. |
| `marts` (default) | `table` | The actual deliverable, queried repeatedly downstream (a BI tool, an analyst). Worth paying the build cost once. |

One nuance worth calling out explicitly: `dbt snapshot` is treated as a
separate step from `dbt build`, both here and in the Airflow DAG (see
[Orchestration](#orchestration)) — but that separation isn't automatic.
dbt-core 1.12 (what `dbt-core==1.*` resolves to as of this writing)
includes snapshots in `dbt build`'s own node graph by default, confirmed
by actually running it: the snapshot ran a second time, silently, inside
`dbt build` the first time this was tested end to end against a real
database. Both the setup instructions above and the Airflow DAG pass
`--exclude "resource_type:snapshot"` to `dbt build` specifically so the
separation is genuinely enforced, not just asserted in prose.

### Business question definitions

- **MRR by month and plan** (`mart_mrr_by_month_plan`): sum of
  `monthly_price` across every subscription active at any point during
  that calendar month. This is a subscription-based recurring-revenue
  view, not a cash view — it does not equal that month's actual collected
  invoice total, on purpose. "Active this month" is resolved once in
  `int_subscriptions__monthly` (a date-spine join) so every consumer of
  that definition agrees.
- **Customer LTV** (`mart_customer_ltv`): realized revenue to date — the
  sum of *paid* invoices only, not a predictive/forward-looking LTV.
  Nothing in this dataset gives grounds to honestly estimate an expected
  churn rate or remaining lifetime, so this mart reports what's actually
  been collected, in EUR, per customer, alongside their country, plan mix
  (every distinct plan they've ever held), and current subscription status
  (their most recent subscription's status, by `start_date`). Pending
  (`open`) and at-risk (`failed`) revenue are surfaced as separate columns
  rather than folded into `ltv`, so the number never silently overstates
  money that hasn't actually been collected.
- **Churn** (`mart_churn`): two rates per month, both measured against the
  *prior* month's active base (not the current month's own activity, which
  would conflate new signups with retention) — `subscription_churn_rate`
  (count-based) and `mrr_churn_rate` (revenue-weighted). Both are kept
  because they can genuinely disagree: losing five cheap `starter`
  subscriptions is a very different problem than losing one `scale`
  subscription, and a count-only rate can't tell those apart.

### FX handling

Two invoices are in SEK; everything else is EUR. `macros/convert_to_eur.sql`
converts at a fixed, illustrative rate (~0.088 EUR/SEK) with an explicit
comment that this is **not** production-grade — a real pipeline would join
each invoice to a daily FX rate table (or a live rate API) keyed by
`invoice_date` and `currency`, since a hardcoded constant drifts wrong the
moment the real rate moves. Fine for two known historical rows in a
take-home; not fine for a real billing pipeline.

### Testing

Testing is treated as a first-class part of this project, per the brief.

- **Generic tests** (`unique`, `not_null`, `accepted_values`,
  `relationships`) are applied wherever they meaningfully apply, at both
  the staging and marts layers.
- **Two custom generic tests** were built as reusable macros rather than
  one-off SQL, since both patterns recur across models:
  - `not_future_date(model, column_name)` — a date column should never be
    in the future. Used on `created_at`.
  - `flag_equals(model, column_name, value)` — asserts a boolean defect
    flag equals a given value (almost always `false`/`true` after a fix).
    Used repeatedly across the `had_..._defect` / `has_valid_...` columns
    instead of writing the same `where flag != expected` SQL five times.
- **Six singular (custom SQL) tests** encode real business rules — well
  past the brief's minimum of two:
  - `assert_stg_customers_row_count_matches_distinct_raw_ids` — staging's
    row count must equal the raw table's distinct business-key count.
    Generic on purpose: it keeps working if a future dedup regression
    changes the exact number, instead of hardcoding today's count.
  - `assert_subscriptions_end_date_not_before_start_date` — a subscription
    must never end before it starts. Verifies the #8 fix actually holds.
  - `assert_no_invoices_after_subscription_end` — an invoice must never
    postdate its subscription's effective end. Verifies the #8/#14 fix
    (this is the test that originally disproved the swap-based fix — see
    [Data-quality findings](#data-quality-findings)).
  - `assert_effective_end_date_not_before_start_date` — sanity check on
    our own derivation logic in `int_subscriptions__effective_dates`, not
    a known raw-data defect.
  - `assert_mrr_never_negative` / `assert_ltv_never_negative` — MRR and
    LTV must never go negative. The kind of thing that should be
    structurally impossible in billing data, worth asserting explicitly
    rather than assuming.
  - `assert_churn_rates_within_valid_range` — both churn rates must fall
    within `[0, 1]`.
- **Severity is deliberate, not default.** Tests that exist specifically to
  *prove a known, already-handled defect is still visible* (e.g. the
  orphan-customer `relationships` test, the `flag_equals` defect flags) are
  `severity: warn` — their job is to keep the defect visible in `dbt build`
  output, not to fail the build over something already decided and
  documented. Tests asserting something that should be structurally true
  regardless of any known defect (no invoice after subscription end, no
  negative MRR/LTV, valid churn range) are left at the default `error`
  severity, since a failure there would mean an actual regression, not an
  already-known and accepted condition.

## Data-quality findings

Full findings and method are in [`docs/DATA_QUALITY.md`](docs/DATA_QUALITY.md)
(written *before* any modeling, per the brief's warning about planted
issues). Summary — 13 genuine defects, all caught by a staging/raw-layer
test, all handled so the marts themselves test clean:

| # | Table | Issue | Handling |
|---|---|---|---|
| 1 | customers | Exact duplicate row (`C0023`) | Deduped — `row_number()` partitioned by business key, kept lowest `_row_id`. |
| 2 | customers | Blank `country` | Normalized to `NULL`, not a sentinel value. |
| 3 | customers | Future `created_at` (`C0041`, 2027) | Left as-is, caught by `not_future_date` (`warn`). This is the same row that poisoned the dlt incremental cursor in step 0 — see below. |
| 4 | subscriptions | Exact duplicate row (`S00006`) | Deduped, same pattern as #1. |
| 5 | subscriptions | Status casing (`ACTIVE`) | Lowercased + trimmed. |
| 6 | subscriptions | Negative price (`S00048`, `-99.00`) | Fixed via `abs()`; flagged via `had_negative_price_defect` rather than silently corrected. |
| 7 | subscriptions | Orphan `customer_id` (`S00011` -> `C9999`) | Kept, not dropped — a real invoiced subscription shouldn't silently vanish from revenue over a referential gap. Flagged via `has_valid_customer`. |
| 8 | subscriptions | `end_date` before `start_date` (`S00034`) | `end_date` nulled out. An earlier swap-based fix (assuming a transposition) was tried and **disproven** by S00034's 17 real monthly invoices spanning 2025-03-29 through 2026-07-28 — swapping would just relocate the same broken value. Nulling is the honest outcome: we don't actually know this subscription's true end date. A last-invoice-date fallback in `int_subscriptions__effective_dates` recovers a usable effective end date for downstream marts. |
| 9 | invoices | Blank `amount` (`I000322`) | Left `NULL` — no value to recover or infer. Correctly excluded from any `SUM(amount)`-based total. Flagged via `had_missing_amount_defect`. |
| 10 | invoices | Non-EUR currency (2 rows, `SEK`) | Converted via `convert_to_eur()` (see FX handling above). |
| 11 | invoices | Status casing + whitespace (`"PAID "`) | Lowercased + trimmed. |
| 12 | invoices | Negative amount (7 rows) | Cascades from #6 (all tied to `S00048`) — fixed via `abs()`, same pattern, flagged via `had_negative_amount_defect`. |
| 13 | invoices | Orphan `subscription_id` (`I000601` -> `S99999`) | Kept, not dropped, same reasoning as #7. Flagged via `has_valid_subscription`. |

Two things cascade rather than being independent defects: #12 is the
invoice-level symptom of #6, and the 17 rows of "invoice after
subscription end" are the invoice-level symptom of #8 — both clear up
once the root cause is fixed upstream, verified by
`assert_no_invoices_after_subscription_end` rather than assumed.

Two things looked suspicious during profiling but checked out as
legitimate, and are called out in `DATA_QUALITY.md` so they don't get
mistakenly re-flagged: blank `end_date` on 123 rows (every one belongs to
an `active` or `paused` subscription, never `cancelled`), and `S00149`
having zero invoices (its `start_date` is genuinely in the future — a
brand-new subscription, not a defect).

### A defect that broke more than a test: the incremental cursor

Beyond the 13 planted defects, running `ingestion/demo_incremental.py`
surfaced a real design flaw in step 0, documented in full in
[`docs/INGESTION.md`](docs/INGESTION.md): the same future-dated
`created_at` on `C0041` (defect #3) became dlt's incremental sync
watermark, causing every *subsequent legitimate update* to look "older"
than what had already been synced — silently skipped, no error. A single
planted row broke sync correctness upstream of dbt entirely, before any
`dbt test` ever got a chance to catch it. Kept exactly as it ran (not
patched around) because it's a more honest and more informative record of
what the design choice actually causes than a clean demo would be.

## Orchestration

`airflow/dags/nordstack_dbt_dag.py` runs the pipeline every 5 minutes:
`dbt snapshot` as its own task first (kept separate from the build, same
reasoning as above), then the staging/intermediate/marts build+test via a
plain `BashOperator` running `dbt build`. Exactly one email fires per run:
success if everything passed, failure if anything didn't.

This DAG was originally built with
[astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/)'s
`DbtTaskGroup` instead — one Airflow task per dbt model/test — to match
how NordStack's real Airflow deployment orchestrates dbt, not because this
project's model count needs that granularity to run reliably. It was
swapped for a plain `BashOperator` after hitting a concrete, verified
environment blocker: Cosmos pulls in `openlineage-sql`, a Rust-compiled
transitive dependency with no prebuilt Windows wheel for any version,
requiring a full C++ build toolchain to compile from source. That's a
Windows-development-environment issue, not a flaw in the Cosmos-based
design — the original `DbtTaskGroup` version still lives in git history,
and the exact tradeoff (plus how to switch back on Linux/macOS or with
the build tools installed) is documented in full in
[`airflow/README.md`](airflow/README.md).

`BashOperator` itself hits a second, separate Windows-only wall: native
`airflow dags test` fails with `ValueError: preexec_fn is not supported
on Windows platforms`, traced to `SubprocessHook.run_command` in
Airflow's own source unconditionally passing `preexec_fn` into
`subprocess.Popen` — not fixable from this project's side, and not
specific to this DAG (no `BashOperator` running any command works on
native Windows). Rather than leave that as a documented-but-unverified
limitation, the DAG was actually run end to end via Docker instead — see
[`airflow/README.md`'s "Running this on Windows"](airflow/README.md#running-this-on-windows)
section for the full story and commands. **Result:** a genuine, live
`airflow dags test` run completed in full — `dbt_snapshot` and `dbt_build`
both passed (59 nodes, same planted-defect pattern as everywhere else in
this project), and `notify_success` sent a real email, confirmed sitting
in a local SMTP catcher's inbox. This is real end-to-end proof, not just
a design argued for on paper.

## What I'd do next with more time

- **Real FX rates.** Replace the hardcoded SEK/EUR constant in
  `convert_to_eur()` with a join against a daily FX rate table keyed by
  `invoice_date` and `currency` — the macro's own comment already flags
  this as the honest limitation of a take-home dataset.
- **Point-in-time MRR via the snapshot.** `int_subscriptions__monthly`
  deliberately doesn't use `subscriptions_snapshot` yet, since nothing in
  this dataset has an actual plan/price change to demonstrate it against.
  With more history, MRR-by-month should resolve each subscription's
  plan/price *as of that month* from the snapshot, rather than assuming
  it never changed.
- **Ingestion-side cursor safeguard.** Per `docs/INGESTION.md`, a
  production version of the dlt pipeline should clamp or quarantine any
  `updated_at` value greater than the current sync time before it can
  update the incremental watermark, so one bad row can't silently stall
  sync for everything after it.
- **`dbt_utils` for the duplicate-row tests.** The two exact-duplicate-row
  defects (#1, #4) are currently caught implicitly by the row-count
  singular test rather than an explicit
  `dbt_utils.unique_combination_of_columns` generic test — worth adding
  for a more direct, self-documenting assertion.
- **Source freshness checks.** `_billing__sources.yml` declares the raw
  sources but doesn't yet configure `loaded_at_field` / freshness
  thresholds — useful now that there's a recurring 5-minute sync to
  monitor for staleness, not just correctness.
