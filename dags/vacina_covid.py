
"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

import requests
import json
from requests.auth import HTTPBasicAuth     
import pandas as pd
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import s3fs


with DAG(
    'vacina_covid',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG vacina covid-19 no Brasil',
    schedule_interval=None,
    start_date=datetime(2021, 5, 16),
    catchup=False,
) as dag:
   
    def request_vacinas():
        req = requests.get('https://imunizacao-es.saude.gov.br/desc-imunizacao/_search', auth=('imunizacao_public', 'qlto5t&7r_@+#Tlstigi'))  
        req_dict = json.loads(req.text)
        res = pd.json_normalize(req_dict)
        df_res = pd.DataFrame(res)
        df_res.rename(columns={"hits.hits": 'hits'}, inplace=True)
        df_hits = df_res[['hits']]
        qq = pd.DataFrame(df_hits['hits'].explode().tolist())
        df_vacinas = pd.json_normalize(qq._source)

        client = boto3.client(
            's3',
            aws_access_key_id = 'aws_access_key_id',
            aws_secret_access_key = 'aws_secret_access_key',
            region_name = 'region_name'
        )

        location = {'LocationConstraint': 'region_name'}
        client.create_bucket(
            Bucket='bucket',
            CreateBucketConfiguration=location
        )

        df_vacinas.to_csv("s3://bucket/df_bucket.csv", index=False,
        storage_options={'key': 'aws_access_key_id',
        'secret': 'aws_secret_access_key'})


    def request_vacinas_s3():
        client = boto3.client(
            's3',
            aws_access_key_id = 'aws_access_key_id',
            aws_secret_access_key = 'aws_secret_access_key',
            region_name = 'us-east-1'
        )

        obj = client.get_object(
            Bucket = 'bucket',
            Key = 'df_bucket.csv'
        )
    
        df_vacinas_s3 = pd.read_csv(obj['Body'])
        print('head df_vacinas_s3: ', df_vacinas_s3)


    request_vacinas_task = PythonOperator(
        task_id='request_vacinas',
        python_callable=request_vacinas,
    )

    request_vacinas_s3_task = PythonOperator(
        task_id='request_vacinas_s3',
        python_callable=request_vacinas_s3,
    )

    request_vacinas_task >> request_vacinas_s3_task 

