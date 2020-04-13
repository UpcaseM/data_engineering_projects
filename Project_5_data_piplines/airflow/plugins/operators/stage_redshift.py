from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import date

class StageToRedshiftOperator(BaseOperator):
    today = date.today()
    ui_color = '#358140'
    sql = '''
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            {}
          '''
    @apply_defaults
    def __init__(self,
                 table='',
                 aws_credentials_id='',
                 region = '',
                 s3_bucket='',
                 s3_key='',
                 additional = '',
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        # Additional for the copy statement
        self.additional = additional
        self.execution_date = kwargs.get('execution_date')
        

    def execute(self, context):
        """
            Copy data from S3 to redshift cluster into staging table.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clear table before insert
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f'TRUNCATE {self.table}')
        
        self.log.info("Copying data from S3 to Redshift")
        path_s3 = f's3://{self.s3_bucket}/{self.s3_key}'
        # Backfill for log_data
        #the execution_date (pendulum.Pendulum)
        if self.s3_key == 'log_data':
            year = self.execution_date.year
            month = self.execution_date.month
            path_s3 = '/'.join([path_s3, str(year), str(month)])
        sql = StageToRedshiftOperator.sql.format(self.table,
                                                 path_s3,
                                                 credentials.access_key,
                                                 credentials.secret_key,
                                                 self.region,
                                                 self.additional
                                                 )
        redshift.run(sql)
        self.log.info(f'{self.table} is copied to redshift!')



