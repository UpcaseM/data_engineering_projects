from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = '',
                 sql = '',
                 redshift_conn_id = '',
                 truncate = True, 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate

    def execute(self, context):
        '''
        Load fact table.
        '''
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        
        # Clear table before if self.truncate is true
        if self.truncate:
            self.log.info(f'Clearing data from {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        redshift.run(str(self.sql))
        self.log.info('Fact table is loaded.')