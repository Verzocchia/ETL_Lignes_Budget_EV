from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection, Variable

from datetime import datetime
import boto3
import os 


def dl_files_from_s3():

    s3_id = Variable.get("hub_coll_id")
    s3_mdp = Variable.get("hub_coll_secret")
    s3_endpoint = Variable.get("hub_coll_endpoint")

    bucket_name = 'actes-budgetaires'
    date_osef = '20240328/'

    session = boto3.Session(
        aws_access_key_id= s3_id,
        aws_secret_access_key= s3_mdp,
        )
    s3 = session.client('s3', verify = False,
                        endpoint_url = s3_endpoint)

    response = s3.list_objects(Bucket=bucket_name, Prefix=date_osef)
    
    if 'Contents' in response:
      for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.xml.gz'):
            id_file = key.split('/')[1]
            print(key)
            chemin_local = os.path.join('./data_test/fichier_test_boto/', f'{id_file}.xml.gz')
            s3.download_file(bucket_name, key, chemin_local)

    else : 
      print("Pas de données aujoud'hui")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
}

dag = DAG(
    'dl_files',
    default_args=default_args,
    description='blabla',
    schedule_interval='@daily',
)

dl_files_from_s3_task = PythonOperator(
    task_id='dl_files_from_s3',
    python_callable=dl_files_from_s3,
    dag=dag,
)

dl_files_from_s3_task