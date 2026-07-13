In Apache Airflow 3, a Hook is a high-level, reusable interface that abstracts the details of interacting with external platforms, databases, and APIs. Instead of writing boilerplate code to handle authentication, network protocols, or connection pools, you use a Hook to let Airflow manage those details via its secure Connection engine.  
While Operators execute a specific action (e.g., S3CopyObjectOperator), Hooks are the internal components that do the actual communication. They are primarily used inside Python functions within TaskFlow tasks (@task), custom operators, or custom sensor definitions.

## Commonly Used Hooks
### 1. SQLExecuteQueryHook (Generic Database Interactions)
In modern Airflow architecture, specific database hooks (like PostgresHook or MySqlHook) inherit from the common SQL interface. Use this hook inside a @task to query or modify data tables.
```python
from airflow.decorators import dag, task
from airflow.providers.common.sql.hooks.sql import SQLExecuteQueryHook
from datetime import datetime

@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def database_pipeline():
    @task
    def fetch_active_accounts():
        # Authenticates securely via the connection ID
        hook = SQLExecuteQueryHook(sql_conn_id="my_postgres_conn")
        records = hook.get_records(sql="SELECT email FROM users WHERE active = true;")
        return [row[0] for row in records]

    fetch_active_accounts()

database_pipeline()
```
### 2. HttpHook (Interacting with External REST APIs)
The HttpHook simplifies making GET, POST, or other HTTP requests by automatically injecting headers, base URLs, and authentication tokens saved in Airflow Connections.
```python
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime

@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def api_pipeline():    
    @task
    def fetch_weather_data():
        # Connects using the base URL and tokens defined in 'weather_api'
        http_hook = HttpHook(http_conn_id="weather_api", method="GET")
        response = http_hook.run(endpoint="v1/forecast?city=mumbai")
        return response.json()

    fetch_weather_data()

api_pipeline()
```
### 3. S3Hook (Managing Cloud Files and Storage)
The S3Hook abstracts away Amazon Web Services (AWS) Boto3 SDK boilerplate logic. It allows you to rapidly check for files, read strings, or upload logs to AWS S3.
```python
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def cloud_storage_pipeline():
    @task
    def upload_report():
        # Leverages underlying AWS credentials safely
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_string(
            string_data="Report summary for 2026.",
            key="reports/summary.txt",
            bucket_name="my-company-bucket",
            replace=True
        )

    upload_report()

cloud_storage_pipeline()
```
