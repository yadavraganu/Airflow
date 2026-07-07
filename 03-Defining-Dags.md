The recommended method to create workflows in Airflow 3 is the TaskFlow API using decorators.  
Below is a complete guide to defining DAGs using the new Airflow 3 Task SDK.

### 1. TaskFlow API Decorators
The most Pythonic and modern way to author pipelines uses @dag and @task. Data dependency (XCom) is managed automatically by passing inputs and outputs like native Python functions.
```python
import pendulum
from airflow.sdk import dag, task  # Unified Airflow 3 SDK namespace
# Define the DAG configuration via the decorator
@dag(
    dag_id="airflow3_sdk_taskflow_demo",
    schedule=None,                  # Manual execution, or use cron e.g., "0 0 * * *"
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,                  # Prevents backfilling omitted historical runs
    tags=["sdk", "airflow3"],
)
def my_workflow():

    @task
    def extract_data() -> dict:
        """Simulates data retrieval from an external system."""
        return {"user_id": 42, "status": "active", "tier": "premium"}

    @task
    def process_user_tier(user_info: dict) -> str:
        """Processes data received implicitly via Airflow XCom."""
        if user_info["tier"] == "premium":
            return f"User {user_info['user_id']} given high-priority processing."
        return f"User {user_info['user_id']} given standard processing."

    @task
    def load_log(message: str):
        """Final sink operation."""
        print(f"Final Pipeline Output: {message}")

    # Establish data and execution dependency implicitly by passing variables
    extracted_payload = extract_data()
    processed_message = process_user_tier(extracted_payload)
    load_log(processed_message)
# CRITICAL: Always invoke the function at the top level to instantiate the DAG
my_workflow()
```
### 2. SDK Context Manager
If you are transitioning legacy pipelines or prefer using traditional standalone operators (e.g., standard BashOperator), use the DAG class from the SDK with a Python context manager. [6, 7, 8] 
```python
import pendulum
from airflow.sdk import DAG  # Core SDK class
from airflow.providers.standard.operators.python import PythonOperator

def _extract_logic():
    return "Raw data stream"

def _transform_logic(ti):
    # Airflow 3 Task instances pull data securely via the Execution API contract
    upstream_data = ti.xcom_pull(task_ids="extract_step")
    return f"Transformed: {upstream_data}"

# Instantiating the pipeline structural skeleton

with DAG(
    dag_id="airflow3_sdk_traditional_demo",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    extract_step = PythonOperator(
        task_id="extract_step",
        python_callable=_extract_logic
    )

    transform_step = PythonOperator(
        task_id="transform_step",
        python_callable=_transform_logic
    )

    # Set exact control-flow boundaries using bitshift syntax
    extract_step >> transform_step
```
## Best Practices for Airflow 3 SDK DAGs

* **Top-Level Code Restraint**: The Airflow DAG File Processor parses these python scripts continuously. Never write API requests or database calls directly in the script body; always confine heavy execution logic safely inside a `@task` body.
* **Function Invocation**: When using the `@dag` decorator, failing to invoke the function at the end of your file (`my_workflow()`) means the scheduler will completely ignore the DAG.
* **Dynamic Scoping**: If utilizing advanced pipeline control setups like grouping related modules together, use `from airflow.sdk import task_group`.
