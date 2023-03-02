from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert_statement = '''
    INSERT INTO {}
    {};
    '''

    @apply_defaults
    def __init__(self,
                 table_name = "",
                 redshift_conn_id = "",
                 sql_insert_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name,
        self.redshift_conn_id = redshift_conn_id,
        self.sql_insert_statement = sql_insert_statement
        
    def execute(self, context):
        # Instantiating the Redshift Hook
        redshift = PostgresHook(postgres_con_id=self.redshift_conn_id)
        
        self.log.info('Inserting data into fact table')
        
        sql_query = LoadFactOperator.sql_insert_statement.format(
            self.table_name,
            self.sql_insert_statement
        )
        redshift.run(sql_query)
        self.log.info('Data inserted into fact table.')
#         self.log.info('LoadFactOperator not implemented yet')
