from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.apple_auth import AppleAuthManager
from airflow.models import Variable

JWT_PATH = "/home/nathan.carter/airflow/dags/secrets/apple_jwt.txt" # move this to a variable in airflow UI
PRIVATE_KEY_PATH = "/home/nathan.carter/airflow/dags/secrets/apple_private_key.p8" # move this to a variable in airflow UI

TEAM_ID = Variable.get("APPLE_TEAM_ID")
KEY_ID = Variable.get("APPLE_KEY_ID")

# Define dataset
JWT_DATASET = Dataset("dataset://apple/jwt")

def check_and_generate_jwt(**kwargs):
    auth_manager = AppleAuthManager(
        team_id=TEAM_ID,
        key_id=KEY_ID,
        private_key_path=PRIVATE_KEY_PATH,
        jwt_store_path=JWT_PATH,
    )
    token = auth_manager.get_valid_token()
    kwargs['ti'].xcom_push(key="apple_jwt", value=token)
    return token

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="apple_music_token_generation",
    start_date=datetime(2025, 7, 30),
    schedule="30 12 * * 5",   # Fridays 8:30am
    catchup=False,
    default_args=default_args,
    tags=["apple", "jwt"],
) as dag:

    generate_token = PythonOperator(
        task_id="generate_jwt_if_needed",
        python_callable=check_and_generate_jwt,
        outlets=[JWT_DATASET]
    )
