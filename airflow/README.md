# Orchestration: running the dbt project on a schedule

`dags/nordstack_dbt_dag.py` runs `dbt snapshot` then `dbt build` every 5
minutes, emailing exactly one notification per run — success or failure.
See the DAG file's own docstring for the full reasoning behind each
design choice (why snapshot is a separate task, why the email pattern is
trigger-rule-based rather than a DAG-level callback, and — below — why
this runs `dbt build` through a plain BashOperator rather than
[astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/)).

## Why BashOperator instead of Cosmos

This DAG was originally built with Cosmos' `DbtTaskGroup` — one Airflow
task per dbt model/test — specifically to match how NordStack's real
Airflow deployment orchestrates dbt, not because this project's ~11
models need that granularity to run reliably. That version is still in
this repo's git history (the commit immediately before the one that
introduced this BashOperator version) rather than deleted.

It was swapped out after hitting a concrete, verified environment
blocker on Windows: Cosmos pulls in `openlineage-integration-common` (for
dbt-run lineage emission, a feature this project doesn't use) as a base
dependency, which in turn depends on `openlineage-sql` — a Rust-compiled
package with **no prebuilt wheel published for Windows, for any version**
(`pip install --only-binary=:all: openlineage-sql` returns zero
candidates). Building it from source needs a Rust toolchain (which
auto-installs via `maturin`) *and* the MSVC linker (`link.exe`), which
means installing the Visual Studio C++ Build Tools — a real, but
avoidable, one-time cost that didn't seem worth spending the remaining
assessment time on. On a Linux-based production Airflow deployment (the
normal target for Cosmos, and almost certainly what NordStack actually
runs), this problem doesn't exist — prebuilt Linux wheels for
`openlineage-sql` are published, so this is a Windows-development-
environment issue, not a real limitation of the design.

To switch back to the Cosmos version:

```bash
# One-time, if on Windows: install the Visual Studio C++ Build Tools
winget install --id Microsoft.VisualStudio.2022.BuildTools -e --override \
  "--wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"

# Uncomment astronomer-cosmos in airflow/requirements-airflow.txt, then:
pip install -r airflow/requirements-airflow.txt

# Restore the DbtTaskGroup version of the DAG from git history:
git log --oneline -- airflow/dags/nordstack_dbt_dag.py   # find the Cosmos commit
git show <that-commit-hash>:airflow/dags/nordstack_dbt_dag.py > airflow/dags/nordstack_dbt_dag.py
```

## Running this on Windows

Falling back to `BashOperator` (above) sidesteps the Cosmos/openlineage-sql
install problem, but it doesn't fully clear the path on native Windows.
`airflow dags test nordstack_dbt_pipeline 2024-01-01` still fails outright
with `ValueError: preexec_fn is not supported on Windows platforms`.

Root cause, confirmed by reading the actual installed
`airflow/hooks/subprocess.py`: `SubprocessHook.run_command` unconditionally
passes `preexec_fn=pre_exec` into `subprocess.Popen`, and Python itself
refuses that argument on native Windows. This isn't specific to this DAG,
or even to Cosmos -- it means **no** `BashOperator`, running **any**
command, works on native Windows. Airflow's own startup warning says as
much: Windows isn't a supported target, WSL2/Linux/macOS is.

Rather than leave that as a documented-but-unverified limitation, this DAG
was actually run end to end via Docker instead of fighting the platform
further. `docker-compose.airflow.yml` (repo root) is layered on top of the
existing `docker-compose.yml` — which stays exactly as given, untouched —
via:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d postgres mailhog
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow \
  airflow dags test nordstack_dbt_pipeline 2024-01-01
```

This runs Airflow's own official image (`apache/airflow:2.11.2-python3.11`)
against the same Postgres service, with dbt installed into the container
via that image's documented `_PIP_ADDITIONAL_REQUIREMENTS` dev/test-only
env var (see the compose file's own comments — this is explicitly not
something to use in production, a real deployment would bake a custom
image instead). `airflow/docker/profiles.yml` points the container's dbt
at Postgres by its Docker-internal service name, not the host port
remap most local dev needs (see that file's own comment).

A `mailhog` container comes up alongside Airflow for exactly one reason:
so the alert email can actually be *seen* landing in an inbox, not just
trusted from a log line saying it was sent.

Running this for real also surfaced a genuine dbt-core 1.12 behavior
change: `dbt build` includes snapshots in its node graph by default now,
which isn't what this project's docs originally assumed (see the "One
nuance worth calling out explicitly" note in the root README). The
`dbt_build` task below excludes snapshots explicitly so the two-step
snapshot-then-build design documented throughout this project actually
holds at runtime:

```bash
dbt build --project-dir nordstack_analytics --profiles-dir . \
  --exclude "resource_type:snapshot"
```

**Result:** `airflow dags test nordstack_dbt_pipeline 2024-01-01` completed
in full — `dbt_snapshot` passed on its own (`PASS=1 WARN=0 ERROR=0
TOTAL=1`), `dbt_build` passed all 58 non-snapshot nodes with the exact
same planted-defect pattern documented throughout this project
(`PASS=50 WARN=8 ERROR=0 TOTAL=58` — the same 51 passes and 8 warnings as
before the split, just with 1 pass now counted under `dbt_snapshot`
instead of `dbt_build`), and `notify_success` genuinely sent an email —
confirmed by opening mailhog's inbox at `http://localhost:8025` and
seeing it sitting there: from `airflow@nordstack.local`, subject
"NordStack dbt pipeline succeeded (2024-01-01
2024-01-01T00:00:00+00:00)". This is real, live, end-to-end proof the
DAG works, not a design that's only ever been argued for on paper.

On a machine where port 5432 or 8080 is already taken (true of this
author's own second machine, which had unrelated Postgres/Airflow
containers already running), remap ports in a gitignored
`docker-compose.override.yml` rather than editing either committed compose
file — same pattern as the existing `docker-compose.override.yml` used for
the plain Postgres/MySQL setup. Compose concatenates `ports:` lists across
merged files rather than replacing them, so an override needs the explicit
`!override` merge tag to actually replace rather than append:

```yaml
services:
  postgres:
    ports: !override []          # no host port needed; airflow reaches it internally as "postgres:5432"
  airflow:
    ports: !override
      - "8081:8080"
```

Tear the whole thing down when done (this only ever touches containers
from these two compose files, never anything else running on the machine):

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v
```

This Docker-based path was chosen over pursuing a native, non-Docker
verification on a different OS (where `preexec_fn` isn't a problem) for a
deliberate reason, not just convenience: it's arguably the stronger
artifact anyway — isolated, reproducible, and closer to how Airflow
actually runs in a real deployment than bare-metal Airflow on a laptop.

## Why Airflow gets its own virtual environment

Airflow is **not** installed into this project's main `.venv` — it gets
its own, separate one (`airflow/.venv-airflow` below). This isn't a
convenience choice: every Airflow 2.x release hard-pins
`sqlalchemy<2.0`, and this project's dbt/dlt stack needs
`sqlalchemy==2.*`. Those two constraints can't be satisfied in the same
environment, on any OS — trying `pip install -r requirements.txt` with
`apache-airflow` added to it fails with a `ResolutionImpossible` error
for exactly this reason. Loosening the project's sqlalchemy pin instead
isn't a real option either, since that risks destabilizing the dbt/dlt
pipeline the rest of this repo is built and tested against.

The split costs nothing functionally: Airflow's `BashOperator` tasks
just shell out to the `dbt` executable by absolute path (see
`DBT_EXECUTABLE_PATH` below), so Airflow's environment never needs
`dbt-core`, `dlt`, or anything from the main `requirements.txt`
installed at all.

## Prerequisites

A **separate** virtual environment from the rest of this project:

```bash
python -m venv airflow/.venv-airflow
source airflow/.venv-airflow/bin/activate   # or airflow\.venv-airflow\Scripts\activate on Windows
pip install -r airflow/requirements-airflow.txt
```

Airflow's own installation is usually done against its published
constraints file, to avoid pip resolving a combination of transitive
dependency versions that don't actually work together — if the plain
install above fails to resolve or behaves oddly, use:

```bash
AIRFLOW_VERSION=2.11.2
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Then point `DBT_EXECUTABLE_PATH` (see Environment variables below) at
the `dbt` inside the *main* project's `.venv` — Airflow's own venv
doesn't have one, by design.

## Point Airflow at this repo's DAG

Rather than copying/symlinking the DAG file into a separate
`~/airflow/dags` folder (another copy to keep in sync), point Airflow's
`dags_folder` directly at this repo:

```bash
export AIRFLOW_HOME="$HOME/airflow"          # or wherever you prefer
airflow db migrate                            # first-time metadata DB setup
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"   # run from repo root
```

## Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `NORDSTACK_ALERT_EMAIL` | **yes** | placeholder (fails loudly) | who receives the success/failure email |
| `DBT_PROJECT_DIR` | no | `<repo_root>/nordstack_analytics` | only needed if the DAG file is relocated relative to the project |
| `DBT_PROFILES_DIR` | no | `~/.dbt` | matches the setup in the main README |
| `DBT_EXECUTABLE_PATH` | **yes**, in practice | whatever `dbt` resolves to on `PATH` | Airflow's own venv has no `dbt` installed by design (see above) -- point this at the main project venv's `dbt` (e.g. `/path/to/repo/.venv/bin/dbt`, or `...\\.venv\\Scripts\\dbt.exe` on Windows) |

```bash
export NORDSTACK_ALERT_EMAIL="you@example.com"
```

## SMTP (for the actual email)

Airflow's mail settings are environment config, not something the DAG
file should carry (see its docstring). Two options:

**Real SMTP** (e.g. Gmail with an app password — a regular account
password won't work with 2FA on):

```bash
export AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
export AIRFLOW__SMTP__SMTP_PORT=587
export AIRFLOW__SMTP__SMTP_STARTTLS=True
export AIRFLOW__SMTP__SMTP_USER=you@gmail.com
export AIRFLOW__SMTP__SMTP_PASSWORD=your-16-char-app-password
export AIRFLOW__SMTP__SMTP_MAIL_FROM=you@gmail.com
```

**Local testing without real credentials** — a throwaway SMTP catcher that
just prints what it receives, so you can prove the email fires without
sending anything real:

```bash
python -m smtpd -n -c DebuggingServer localhost:1025
export AIRFLOW__SMTP__SMTP_HOST=localhost
export AIRFLOW__SMTP__SMTP_PORT=1025
export AIRFLOW__SMTP__SMTP_STARTTLS=False
export AIRFLOW__SMTP__SMTP_SSL=False
```

## Run it

Smoke-test the DAG without leaving a webserver running — this alone
proves the DAG parses and the whole pipeline executes end to end:

```bash
airflow dags test nordstack_dbt_pipeline 2024-01-01
```

To see it running on its actual 5-minute schedule with the full UI:

```bash
airflow standalone
```

This prints an auto-generated `admin` password on first run. Open
`localhost:8080`, find `nordstack_dbt_pipeline`, un-pause it (DAGs start
paused by default), and either wait for the next 5-minute tick or trigger
it manually. The Graph view shows `dbt_snapshot` → `dbt_build` →
`notify_success` / `notify_failure`.
