from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from spark_jobs.imdb_project_jobs import *
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
aws_access_key = Variable.get('aws_access_key')
aws_secret_key = Variable.get('aws_secret_key')
path = "local path"
path2 = "local file path"

default_args = {
    'owner': 'dag owner name',
    'depends_on_past': False,
    'retries': 3
}

with DAG(
    dag_id="glue_self_project_imdb",
    default_args=default_args,
    # Dag should start from 21st october 2022.
    start_date=datetime(2022, 10, 21),
    # It is schedule to run at 10:20PM every monday.
    schedule='20 10 * 1 *', #minute hour day month week
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    py_download = PythonOperator(
        task_id = 'download_kaggle_data',
        python_callable = download_data,
        dag = dag
    )

    py_load_rds = PythonOperator(
        task_id = 'load_rds',
        python_callable= local_to_rds,
        dag = dag,
        op_args=[path,path2]
    )
    
    glue_job_step = GlueJobOperator(
        task_id = "glue_job_step",
        job_name = "glue scrpit name ",
        job_desc = f"triggering glue job {'job name'}",
        region_name = "us-east-1",
        iam_role_name = "AWSGlueRole",
        num_of_dpus = 1,
        dag = dag
    )


    end = EmptyOperator(
        task_id='end',
        dag=dag
    )


    start >> py_download >> py_load_rds >> glue_job_step >> end
    # start >> glue_job_step >> end
    # start >> py_download >> end
    # start >> py_load_rds >> end
