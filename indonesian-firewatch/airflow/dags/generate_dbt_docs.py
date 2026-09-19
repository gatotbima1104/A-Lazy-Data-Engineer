from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import dag
from includes.dbt_doc_hash import DbtDocsHash
from includes.tasks.setup import DBT_PROJECT_DIR


def _check_dbt_docs_changes() -> bool:
    checker = DbtDocsHash(DBT_PROJECT_DIR)

    if checker.should_generate():
        print("dbt documentation needs to be generated.")
        return True

    print("dbt documentation is up to date. Skipping.")
    return False

def _update_dbt_docs_hash():
    checker = DbtDocsHash(DBT_PROJECT_DIR)
    checker.update_hash()

    print("dbt documentation hash updated.")


@dag(
    dag_id="generate_dbt_docs",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["dbt", "documentation"],
)
def generate_dbt_docs():

    docs_changed = ShortCircuitOperator(
        task_id="check_dbt_docs_changes",
        python_callable=_check_dbt_docs_changes,
    )

    generate_docs = BashOperator(
        task_id="generate_docs",
        bash_command="dbt docs generate",
        cwd=DBT_PROJECT_DIR,
    )

    update_hash = PythonOperator(
        task_id="update_dbt_docs_hash",
        python_callable=_update_dbt_docs_hash,
    )

    docs_changed >> generate_docs >> update_hash


generate_dbt_docs()