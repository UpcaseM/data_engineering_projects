from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'UpcaseM',
    'start_date': datetime.now(),
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry
    'email_on_retry': False
}

# Get variables
dag_config = Variable.get('sparkify_variables', deserialize_json = True)

dag = DAG('sparkify_dag',
          description='Load and transform data in Redshift with Airflow',
          default_args = default_args,
          max_active_runs=1,
          schedule_interval='0 8 1 * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table=dag_config['lst_table'][0],
    aws_credentials_id=dag_config['aws_credentials'],
    region = dag_config['region'],
    s3_bucket=dag_config['s3_bucket'],
    s3_key=dag_config['s3_key_log'],
    additional = dag_config['additional_log'],
    redshift_conn_id=dag_config['conn_redshift'],
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table=dag_config['lst_table'][1],
    aws_credentials_id=dag_config['aws_credentials'],
    region = dag_config['region'],
    s3_bucket=dag_config['s3_bucket'],
    s3_key=dag_config['s3_key_song'],
    additional = dag_config['additional_song'],
    redshift_conn_id=dag_config['conn_redshift'],
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table = dag_config['lst_table'][2],
    sql = SqlQueries.songplay_table_insert,
    redshift_conn_id = dag_config['conn_redshift'],
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table = dag_config['lst_table'][3], 
    redshift_conn_id = dag_config['conn_redshift'],
    sql = SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table = dag_config['lst_table'][4], 
    redshift_conn_id = dag_config['conn_redshift'],
    sql = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table = dag_config['lst_table'][5], 
    redshift_conn_id = dag_config['conn_redshift'],
    sql = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table = dag_config['lst_table'][6], 
    redshift_conn_id = dag_config['conn_redshift'],
    sql = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables = dag_config['lst_table'][2:],
    redshift_conn_id = dag_config['conn_redshift'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Configure the task dependencies
start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, 
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
