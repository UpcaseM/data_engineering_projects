from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = '', 
                 redshift_conn_id = '',
                 sql = '',
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate
        

    def execute(self, context):
        '''
        Insert data into dimentional tables.
        '''
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Clear table before if self.truncate is true
        if self.truncate:
            self.log.info(f'Clearing data from {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        
        redshift.run(str(self.sql))
        self.log.info(f'Table {self.table} is loaded.')