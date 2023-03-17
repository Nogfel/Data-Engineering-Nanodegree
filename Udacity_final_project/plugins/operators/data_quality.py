from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Starting data quality checks')
       
        # Dictionary with tables and its respective columns 
        # we need to check for NULL values
        checks = {
            'artists':['artistid', 'name'],
            'songplays':['playid'],
            'songs':['songid'],
            'time':['start_time'],
            'users':['userid']
        }
        
        # Creating connetion object withe Redshift
        redshift = PostgresHook(postgres_con_id=self.redshift_conn_id)
        
        # Loop trough the dictionary collecting the information to 
        # perform the data checks
        for key, value in checks.items():
            self.log.info('||| Analysing {} table. |||'.format(key))
            for column in value:
                # Query to check for NULL values on the tables
                null_query_check = 'SELECT COUNT(1) FROM {} WHERE {} IS NULL'.format(key, column)
                
                # Gathering information about NULL values from a column
                data_check = redshift.get_records(null_query_check)
                
                # Reporting an error in case of NULL values or a approval statement for the column/table
                qty_null_values = data_check[0][0]
                if qty_null_values > 0:
                    raise ValueError('Data quality check failed. There are {} NULL values in {} table at {} column.'.format(qty_null_values, key, column))
                else:
                    self.log.info('OK - {} table has no issues on {} column with NULL values.'.format(key, column))        
