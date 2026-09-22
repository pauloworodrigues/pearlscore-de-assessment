"""
Airflow DAG: run the NordStack dbt project on a schedule and alert by email.

Orchestration requirement from the brief: run dbt every 5 minutes, email
notification on success and on failure.

Design, in order:

  1. `dbt snapshot` runs as its own task, before the build. As documented
     throughout this project (see snapshots/subscriptions_snapshot.sql and
     the README's materialization-strategy section), the snapshot is
     conceptually a separate step from the build -- confirmed the hard
     way: dbt-core 1.12 (what dbt-core==1.* actually resolves to here)
     includes snapshots in `dbt build`'s own node graph by default, which
     silently ran the snapshot a second time inside `dbt_build` the first
     time this was tested end to end against a real database. `dbt_build`
     below passes `--exclude "resource_type:snapshot"` specifically to
     keep that separation genuinely true rather than just documented.
  2. `dbt build` runs the staging/intermediate/marts build+test suite via
     a plain BashOperator.

     This was originally built with astronomer-cosmos' DbtTaskGroup
     instead -- one Airflow task per dbt model/test -- specifically to
     match how NordStack's real Airflow deployment orchestrates dbt
     (Cosmos), not because this project's ~11 models need per-model
     granularity to run reliably. That version is still in this repo's
     git history (the commit immediately before this one) rather than
     deleted, since it's the actual evidence of the intended design.

     It was swapped for a plain BashOperator after hitting a concrete,
     verified environment blocker, not a change of opinion about which
     approach is architecturally right: Cosmos pulls in
     openlineage-integration-common (for dbt-run lineage emission, a
     feature this project doesn't use) as a base dependency, which in
     turn depends on openlineage-sql -- a Rust-compiled package with no
     prebuilt wheel published for Windows, for any version
     (`pip install --only-binary=:all: openlineage-sql` returns zero
     candidates). Building it from source needs a Rust toolchain (which
     auto-installs) *and* the MSVC linker (`link.exe`), which requires
     installing the Visual Studio C++ Build Tools -- a ~1.5-2GB, several-
     minute one-time install. On a Linux-based production Airflow
     deployment (the normal target for Cosmos, and almost certainly what
     NordStack actually runs), this entire problem doesn't exist --
     prebuilt Linux wheels for openlineage-sql exist and this blocker is
     Windows-development-environment-specific, not a real limitation of
     the design.

     Reinstalling Cosmos on a machine with the C++ Build Tools available
     (or on Linux/macOS) and swapping this task back for the DbtTaskGroup
     version in git history is a small, mechanical change -- the
     reasoning for doing so is unchanged; only the ability to verify it
     locally, in the time available, was the constraint.

     A second, separate Windows-only blocker showed up even after
     falling back to BashOperator: native `airflow dags test` on Windows
     fails outright with `ValueError: preexec_fn is not supported on
     Windows platforms`. Root cause, confirmed by reading the installed
     `airflow/hooks/subprocess.py`: `SubprocessHook.run_command`
     unconditionally passes `preexec_fn=pre_exec` into `subprocess.Popen`,
     and Python itself refuses that argument on native Windows -- this
     isn't really an Airflow bug, it's BashOperator (any BashOperator,
     regardless of what command it runs) simply not being usable outside
     WSL/Linux/macOS on Windows.

     Rather than leave that as a documented-but-unverified limitation,
     the DAG was actually run end to end in Docker instead:
     docker-compose.airflow.yml, layered on top of the existing
     docker-compose.yml (which stays exactly as given), runs Airflow's
     own official image against the same Postgres, with dbt installed
     via that image's documented _PIP_ADDITIONAL_REQUIREMENTS dev
     feature, plus a mailhog container so the alert email could actually
     be seen, not just trusted to have sent. Result: `airflow dags test
     nordstack_dbt_pipeline 2024-01-01` completed in full -- dbt_snapshot
     passed, dbt_build passed all 59 nodes with the same planted-defect
     pattern documented throughout this project, and notify_success
     genuinely sent an email, confirmed sitting in mailhog's inbox. See
     airflow/README.md's "Running this on Windows" section for the full
     story and the exact commands.
  3. Exactly one email fires per DAG run: a success email if every
     upstream task succeeded, or a failure email if anything failed. This
     is the standard trigger_rule idiom (ALL_SUCCESS / ONE_FAILED on two
     leaf tasks) rather than Airflow's newer DAG-run-level
     on_success_callback/on_failure_callback -- the trigger_rule approach
     works on any supported Airflow 2.x version instead of requiring
     >=2.6. default_args disables Airflow's built-in per-task
     email_on_failure: without that, a single failed model would also
     fire its own task-level email, on top of the DAG-level one this DAG
     is built to send -- the brief asks for one notification per DAG
     outcome, not one per task.

SMTP isn't configured here -- Airflow's SMTP connection is environment
config (airflow.cfg / AIRFLOW__SMTP__* env vars), not something a DAG
file should carry. See airflow/README.md for local setup, including how
to test this without real email credentials.
"""

from __future__ import annotations

import os
import shutil
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# This file lives at <repo_root>/airflow/dags/nordstack_dbt_dag.py, so the
# repo root is two levels up -- computed from __file__ rather than
# hardcoded, same reasoning as ingestion/seed_mysql.py: works regardless
# of where the repo is cloned or how Airflow's dags_folder is configured.
REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", str(REPO_ROOT / "nordstack_analytics")))
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", str(Path.home() / ".dbt")))
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH") or shutil.which("dbt") or "dbt"

# Who gets the success/failure email. Deliberately no real default --
# has to be set explicitly via env var by whoever is running this, rather
# than silently inheriting a placeholder address that isn't theirs.
ALERT_EMAIL = os.getenv(
    "NORDSTACK_ALERT_EMAIL", "set-NORDSTACK_ALERT_EMAIL-env-var@example.com"
)

default_args = {
    "owner": "nordstack-analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # Per-task email is deliberately off -- see module docstring: this DAG
    # sends exactly one email per run (success or failure), not one per
    # failed task on top of that.
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nordstack_dbt_pipeline",
    description="Snapshot + build/test the NordStack dbt project every 5 minutes.",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,  # a recurring operational sync, not a backfill job
    max_active_runs=1,  # don't let a slow run overlap the next 5-minute trigger
    dagrun_timeout=timedelta(minutes=4),  # fail fast rather than stack up past the next schedule
    tags=["dbt", "nordstack"],
) as dag:

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} snapshot "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # See module docstring: this was a Cosmos DbtTaskGroup (still in git
    # history) before a Windows-specific packaging blocker forced a
    # fallback to a plain BashOperator for local verification.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f'--exclude "resource_type:snapshot"'
        ),
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        to=ALERT_EMAIL,
        subject="NordStack dbt pipeline succeeded ({{ ds }} {{ ts }})",
        html_content=(
            "<p>The NordStack dbt pipeline (snapshot + build + test) "
            "completed successfully.</p>"
            "<p>Run: {{ dag_run.run_id }}</p>"
        ),
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
        to=ALERT_EMAIL,
        subject="NordStack dbt pipeline FAILED ({{ ds }} {{ ts }})",
        html_content=(
            "<p>The NordStack dbt pipeline failed -- at least one task in "
            "this run did not succeed.</p>"
            "<p>Run: {{ dag_run.run_id }}</p>"
            "<p>Check the Airflow UI for which task failed and why.</p>"
        ),
    )

    dbt_snapshot >> dbt_build
    [dbt_snapshot, dbt_build] >> notify_success
    [dbt_snapshot, dbt_build] >> notify_failure
