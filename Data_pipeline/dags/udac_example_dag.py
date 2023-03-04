from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 2, 27),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events_to_redshift",
    table="staging_events",
    redshift_conn_id="postgres_default",
    aws_credentials_id="aws_default",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs_to_redshift",
    table="staging_songs",
    redshift_conn_id="postgres_default",
    aws_credentials_id="aws_default",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="JSON 'auto'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_songplays_table',
    table_name = "songplays",
    redshift_conn_id = "postgres_default",
    sql_insert_statement = SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
     task_id='Load_user_dim_table',
     table_name="users",
     redshift_conn_id="postgres_default",
     sql_insert_statement=SqlQueries.user_table_insert,
     dag=dag
 )

load_song_dimension_table = LoadDimensionOperator(
     task_id='Load_song_dim_table',
     table_name="songs",
     redshift_conn_id="postgres_default",
     sql_insert_statement=SqlQueries.song_table_insert,     
     dag=dag
 )


load_artist_dimension_table = LoadDimensionOperator(
     task_id='Load_artist_dim_table',
     table_name="artists",
     redshift_conn_id="postgres_default",
     sql_insert_statement=SqlQueries.artist_table_insert,     
     dag=dag
 )

load_time_dimension_table = LoadDimensionOperator(
     task_id='Load_time_dim_table',
     table_name="time",
     redshift_conn_id="postgres_default",
     sql_insert_statement=SqlQueries.time_table_insert,     
     dag=dag
 )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="postgres_default",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG order execution
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_time_dimension_table 
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table

load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
