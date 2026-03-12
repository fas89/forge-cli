# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Customer 360 Analytics Pipeline - Airflow DAG

This DAG orchestrates the Customer 360 analytics pipeline, ensuring
data freshness and quality for customer segmentation and analytics.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# Default arguments
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "customer_360_pipeline",
    default_args=default_args,
    description="Customer 360 Analytics Pipeline",
    schedule_interval="@daily",
    catchup=False,
    tags=["customer-analytics", "dbt", "customer-360"],
)

# Start task
start = DummyOperator(
    task_id="start",
    dag=dag,
)

# Data quality checks
data_quality_check = BashOperator(
    task_id="data_quality_check",
    bash_command="dbt test --select source:*",
    dag=dag,
)

# Staging layer
run_staging = BashOperator(
    task_id="run_staging",
    bash_command="dbt run --select staging",
    dag=dag,
)

# Test staging
test_staging = BashOperator(
    task_id="test_staging",
    bash_command="dbt test --select staging",
    dag=dag,
)

# Intermediate layer
run_intermediate = BashOperator(
    task_id="run_intermediate",
    bash_command="dbt run --select intermediate",
    dag=dag,
)

# Test intermediate
test_intermediate = BashOperator(
    task_id="test_intermediate",
    bash_command="dbt test --select intermediate",
    dag=dag,
)

# Marts layer
run_marts = BashOperator(
    task_id="run_marts",
    bash_command="dbt run --select marts",
    dag=dag,
)

# Test marts
test_marts = BashOperator(
    task_id="test_marts",
    bash_command="dbt test --select marts",
    dag=dag,
)

# Generate documentation
generate_docs = BashOperator(
    task_id="generate_docs",
    bash_command="dbt docs generate",
    dag=dag,
)

# End task
end = DummyOperator(
    task_id="end",
    dag=dag,
)

# Task dependencies
start >> data_quality_check >> run_staging >> test_staging
test_staging >> run_intermediate >> test_intermediate
test_intermediate >> run_marts >> test_marts
test_marts >> generate_docs >> end
