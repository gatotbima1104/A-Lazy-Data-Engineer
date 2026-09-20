from airflow.providers.standard.operators.bash import BashOperator

from includes.tasks.setup import DBT_PROJECT_DIR


def _dbt_build(
    *,
    task_id: str,
    target: str,  
    **kwargs
):
    """ [DAG] Build dbt model """
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"dbt build --select {target} "
            "--vars "
            "'{\"fire_detection_date\": "
            "\"{{ dag_run.conf.get('fire_detection_date', '') }}\"}'"
        ),
        cwd=DBT_PROJECT_DIR,
        **kwargs
    )
  
def install_packages():
    """ [DAG] Install dbt pakcages """
    return BashOperator(
        task_id="install_dbt_packages",
        bash_command="dbt deps",
        cwd=DBT_PROJECT_DIR
    )
     
def build_staging():
    """ [DAG] Build staging layer """
    return _dbt_build(
        task_id="staging_layer",
        target="staging"
    )
    
def build_intermediate():
    """ [DAG] Build intermediate layer """
    return _dbt_build(
        task_id="intermediate_layer",
        target="intermediate"
    )

def build_marts():
    """ [DAG] Build marts layer """
    return _dbt_build(
        task_id="marts_layer",
        target="marts"
    )