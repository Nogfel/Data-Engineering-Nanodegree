from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

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
        
        # The only way I could make it work was inserting '[0]' the table_name method. Otherwise, the code 
        # was understanding it as a tuple with only one element inside.
        redshift.run("INSERT INTO {} {}".format(self.table_name[0], self.sql_insert_statement))
        
        self.log.info('Data inserted into fact table.')