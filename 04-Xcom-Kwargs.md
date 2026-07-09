In Apache Airflow 3, XComs (cross-communication) and `**kwargs` (task context) are foundational mechanisms used to pass data between tasks and inspect runtime metadata.
You can also bypass `**kwargs` entirely by directly injecting the Task Instance (`ti`) object as a typed argument.

### 1. Core Definitions

* **XCom (Cross-Communication)**: Airflow's internal storage mechanism allowing tasks to share small messages or states (e.g., strings, IDs, small dicts). It writes data directly to the Airflow backend database.
* **`**kwargs (Task Context)`**: A dictionary containing all runtime metadata injected into your Python functions (e.g., run_id, logical_date, ti).
* **ti (Task Instance)**: The specific object representing the current execution of your task. It is the engine that drives XCom pushes, pulls, and provides metadata like retry counts (try_number).
<br></br>
### 2. When & How to Use Them (Airflow 3 Syntax)

#### Scenario A: Modern TaskFlow API (Implicit XCom)

* When to use: For standard, clean Python task pipelines. Returning a value automatically registers an XCom, and passing that returned object downstream pulls it implicitly.
```python
from airflow.decorators import dag, task

@dag(schedule=None, start_date=None)def pipeline():
    
    @task
    def extract_order_id():
        return "ORD-99231"  # Automatically pushed to XCom

    @task
    def process_order(order_id: str):
        print(f"Processing record for: {order_id}")  # Implicitly pulls XCom

    process_order(extract_order_id())

pipeline()
```
#### Scenario B: Explicit ti Injection (Recommended over **kwargs)

* **When to use**: When you need explicit control over XComs, or need to inspect task metadata (like retry attempts or task status) without cluttering code with **kwargs.
* **Crucial Airflow 3 Requirement**: You must explicitly specify task_ids when pulling an XCom from another task to avoid ambiguity.
```python
from airflow.decorators import dag, taskfrom airflow.models.taskinstance import TaskInstance

@dag(schedule=None, start_date=None)def audit_pipeline():

    @task
    def run_audit():
        return {"status": "warning", "count": 1420}

    @task
    def evaluate_results(ti: TaskInstance):
        """Airflow automatically injects 'ti' using this type hint."""
        # Explicit XCom Pull (Requires task_ids in Airflow 3)
        audit_data = ti.xcom_pull(task_ids="run_audit")
        
        # Accessing Task Metadata directly
        print(f"Run ID: {ti.run_id} | Attempt: {ti.try_number}")

        if audit_data and audit_data.get("status") == "warning":
            print(f"Alert raised for {audit_data['count']} items.")

    evaluate_results(ti=run_audit())

audit_pipeline()
```
#### Scenario C: Classic Operator Method using **kwargs

* **When to use**: When working with traditional operators (like PythonOperator) or when you need a wide variety of native Jinja context variables simultaneously.
```python
from airflow.providers.standard.operators.python import PythonOperator
def push_metadata(**kwargs):
    kwargs['ti'].xcom_push(key='file_count', value=42)
def pull_metadata(**kwargs):
    count = kwargs['ti'].xcom_pull(task_ids='producer_task', key='file_count')
    logical_date = kwargs.get('logical_date')
    print(f"Found {count} files on scheduled date: {logical_date}")
# producer_task = PythonOperator(task_id='producer_task', python_callable=push_metadata)
# consumer_task = PythonOperator(task_id='consumer_task', python_callable=pull_metadata)
```
<br></br>
### 3. Quick Reference: When to Avoid

| Mechanism | Avoid When... | Reason & Alternative |
|---|---|---|
| XCom | Large Data Transfers (e.g., Pandas DataFrames, CSVs, large JSON strings). | Bloats the Airflow database and slows performance. Alternative: Upload files to Cloud Storage (S3/GCS) and pass only the S3 URI string via XCom. |
| XCom | Non-Serializable Objects (e.g., open file handlers, live DB connections). | XCom requires data to be JSON-serializable. Complex python objects will cause task failures. |
| **kwargs | Fetching Global Variables. | Avoid parsing kwargs.get('var') for fixed configurations. Alternative: Use native Jinja templates {{ var.value.my_var }}. |
| **kwargs | Clean TaskFlow Decorators. | Using **kwargs randomly obscures your function's real input requirements. Alternative: Type-hint specific objects (like ti: TaskInstance) for IDE autocompletion and easier unit testing. |
