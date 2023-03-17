from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow import create_redshift_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

create_tables = [crt.staging_airport_codes_create_table,
                crt.staging_imigration_create_table,
                crt.staging_sas_create_table,
                crt.dim_country_create,
                crt.dim_state_create,
                crt.dim_modal_create,
                crt.dim_visa_motive_create,
                crt.dim_port_create,
                crt.dim_visa_create]

default_args = {
    'owner': 'nogueira.felipe',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('imgiration_warehouse',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



create_redshift_tables = RedshiftSQLOperator(
        task_id='create_redshift_tables',
        sql=create_tables,
        dag=dag
)

staging_airport_codes_copy = StageToRedshiftOperator(
    task_id='Stage_events',
    table='staging_airport_codes'
    redshift_conn_id="postgres_default",
    aws_credentials_id="aws_default",  
    s3_bucket="nogfel-imigration",
    s3_key="airport_data",
    region="us-west-2",
    delimiter=',',
    dag=dag
)


staging_imigration_copy = StageToRedshiftOperator(
    task_id='Stage_imigration',
    table='staging_imigration'
    redshift_conn_id="postgres_default",
    aws_credentials_id="aws_default",  
    s3_bucket="nogfel-imigration",
    s3_key="imigration_data",
    region="us-west-2",
    delimiter=',',
    dag=dag
)

staging_sas_information_copy = StageToRedshiftOperator(
    task_id='Stage_sas_information',
    table='staging_sas_information'
    redshift_conn_id="postgres_default",
    aws_credentials_id="aws_default",  
    s3_bucket="nogfel-imigration",
    s3_key="sas_data",
    region="us-west-2",
    delimiter='|',
    dag=dag
)

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

load_dim_country = LoadDimensionOperator(
    task_id='load_dim_country',
    table_name="dim_country",
    redshift_conn_id="postgres_default",
    sql_insert_statement=create_redshift_tables.load_dim_country,
    dag=dag
)

load_dim_state = LoadDimensionOperator(
    task_id='load_dim_state',
    table_name="dim_state",
    redshift_conn_id="postgres_default",
    sql_insert_statement=create_redshift_tables.load_dim_state,
    dag=dag
)

load_dim_modal = LoadDimensionOperator(
    task_id='load_dim_modal',
    table_name="dim_modal",
    redshift_conn_id="postgres_default",
    sql_insert_statement=create_redshift_tables.load_dim_modal,
    dag=dag
)

load_dim_visa_motive = LoadDimensionOperator(
    task_id='load_dim_visa_motive',
    table_name="dim_visa_motive",
    redshift_conn_id="postgres_default",
    sql_insert_statement=create_redshift_tables.load_dim_visa_motive,
    dag=dag
)

load_dim_port = LoadDimensionOperator(
    task_id='load_dim_port',
    table_name="dim_port",
    redshift_conn_id="postgres_default",
    sql_insert_statement=create_redshift_tables.load_dim_port,
    dag=dag
)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_redshift_tables 

create_redshift_tables >> staging_airport_codes_copy
create_redshift_tables >> staging_imigration_copy
create_redshift_tables >> staging_sas_information_copy

staging_airport_codes_copy >> load_dim_country
staging_imigration_copy >> load_dim_country
staging_sas_information_copy >> load_dim_country

load_dim_country >> load_dim_state >> load_dim_modal >> load_dim_visa_motive >> load_dim_port