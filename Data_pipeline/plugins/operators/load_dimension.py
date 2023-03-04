from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_insert_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_insert_statement = sql_insert_statement

    def execute(self, context):
        
        self.log.info('Starting job')
        
        self.log.info('Connecting to the database')
        redshift = PostgresHook(postgres_con_id = self.redshift_conn_id)
        
        self.log.info('Truncating tables')
        redshift.run('TRUNCATE {}'.format(self.table_name))
                     
        self.log.info('Updating tables')
        redshift.run('INSERT INTO {} {}'.format(self.table_name, self.sql_insert_statement))
                     
        self.log.info('Update completed')