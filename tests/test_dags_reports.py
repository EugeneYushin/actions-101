"""Test for Airflow DAGs."""
import json
import os
from datetime import datetime

import pytest
from airflow import DAG
from airflow.models import DagBag, DagRun, TaskInstance, Variable
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="session")
def variables():
    """Load Airflow variables."""
    with open(os.path.join(CURRENT_DIR, "../composer/variables.json")) as f:
        variables = json.load(f)
        for key, value in variables.items():
            Variable.set(key, json.dumps(value))


def check_params(params, **kwargs):
    """Check params are merged correctly."""
    kwargs["ti"].xcom_push("query", f'select * from {params["report_prefix"]}{params["table_name"]}')


@pytest.mark.usefixtures("variables")
def test_vars():
    assert Variable.get("REPORTS") == '{"FOO": "bar"}'


@pytest.mark.parametrize(
    ["report_prefix", "expected"],
    [
        (None, "select * from dummy_table"),
        ("test_", "select * from test_dummy_table"),
    ],
    ids=["default-prefix", "custom-prefix"],
)
def test_default_params(report_prefix: str | None, expected: str) -> None:
    """Test params precedence."""
    dag = DAG(
        dag_id="dummy_dag",
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        params={"report_prefix": ""},  # DEFAULT
    )

    t1 = PythonOperator(task_id="dummy_task", python_callable=check_params, params={"table_name": "dummy_table"}, dag=dag)

    dagrun: DagRun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        run_type=DagRunType.MANUAL,
        execution_date=timezone.utcnow(),
        conf={"report_prefix": report_prefix} if report_prefix else None,  # OVERRIDEN
    )

    ti: TaskInstance = dagrun.get_task_instance(t1.task_id)
    ti.task = dag.get_task(task_id=t1.task_id)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(t1.task_id, key="query") == expected
