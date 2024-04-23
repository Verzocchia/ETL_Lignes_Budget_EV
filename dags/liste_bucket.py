from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection, Variable
from datetime import datetime
import boto3


def list_s3_buckets():
    s3_id = Variable.get("hub_coll_id")
    s3_mdp = Variable.get("hub_coll_secret")
    s3_endpoint = Variable.get("hub_coll_endpoint")

    session = boto3.Session(
        aws_access_key_id= s3_id,
        aws_secret_access_key= s3_mdp,
        )
    s3 = session.client('s3', verify = False,
                        endpoint_url = s3_endpoint)

    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    print("Liste des buckets S3 :")
    for bucket in buckets:
        print(bucket)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
}

dag = DAG(
    's3_list_buckets_dag',
    default_args=default_args,
    description='DAG to list S3 buckets',
    schedule_interval='@daily',
)

list_buckets_task = PythonOperator(
    task_id='list_s3_buckets_task',
    python_callable=list_s3_buckets,
    dag=dag,
)

list_buckets_task
