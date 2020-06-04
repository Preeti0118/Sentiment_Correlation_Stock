from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import logging
from botocore.exceptions import ClientError
import os
import zipfile
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import pymysql
import papermill as pm
from airflow.models import Variable


######################
#   Read Variables
######################

var_list = Variable.get("sentiment_variables_ps", deserialize_json=True)
var_news_api_cmd = var_list["var_news_api_cmd"]
var_headlines_api_cmd = var_list["var_headlines_api_cmd"]
var_twitter_api_cmd = var_list["var_twitter_api_cmd"]
var_sentiment_cmd = var_list["var_sentiment_cmd"]
var_csv_mysql_cmd = var_list["var_csv_mysql_cmd"]
var_ticker = var_list["var_ticker"]
var_input_notebook_yfinance = var_list["var_input_notebook_yfinance"]
var_output_notebook_yfinance = var_list["var_output_notebook_yfinance"]
var_input_notebook_correlation = var_list["var_input_notebook_correlation"]
var_output_notebook_correlation = var_list["var_output_notebook_correlation"]
var_input_notebook_visualization = var_list["var_input_notebook_visualization"]
var_output_notebook_visualization = var_list["var_output_notebook_visualization"]

#######################
#   Functions
#######################

def call_yfinance_notebook():
    return True
    pm.execute_notebook(
        var_input_notebook_yfinance,
        var_output_notebook_yfinance,
    #     parameters={
    #                 'ticker': var_ticker,
    #                 }
    )



def call_correlation_visual_notebook():
    pm.execute_notebook(
        var_input_notebook_correlation,
        var_output_notebook_correlation
        # parameters={
        #             'ticker': var_ticker,
        #
        #             }

    )


#########################
#  Dags Pipeline code
#########################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['preetisehgal2001@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    dag_id='sentiment_analysis_data_pipeline_ps',
    default_args=default_args,
    description='sentiment_analysis',
    schedule_interval=timedelta(days=1),
)


t0 = DummyOperator(
        task_id='Start_Pipeline',
        retries=1,
        dag=dag,
)

t1 = BashOperator(
        task_id='run_news_api',
        bash_command=var_news_api_cmd,
        dag=dag,
    )

t2 = BashOperator(
        task_id='run_headlines_api',
        bash_command=var_headlines_api_cmd,
        dag=dag,
    )

t3 = BashOperator(
        task_id='run_twitter_api',
        bash_command=var_twitter_api_cmd,
        dag=dag,
    )

t4 = PythonOperator(
        task_id='run_yfinance_api',
        provide_context=False,
        python_callable=call_yfinance_notebook,
        dag=dag,
)

t5 = BashOperator(
        task_id='sentiment_analysis',
        bash_command=var_sentiment_cmd,
        dag=dag,
    )


t7 = BashOperator(
        task_id='insert_into_sqldb',
        bash_command=var_csv_mysql_cmd,
        dag=dag,
    )

t8 = PythonOperator(
        task_id='correlate_visualization_report',
        provide_context=False,
        python_callable=call_correlation_visual_notebook,
        dag=dag,
)


t0 >> t2 >> t5 >> t7

t0 >> [t1, t3, t4,] >> t7 >> t8
