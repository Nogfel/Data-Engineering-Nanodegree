from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    # Why this `ui_color` variable is necessary?
    ui_color = '#358140'
    
    sql_statement = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION AS '{}'
    {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        # Instantiating the AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        
        # Getting aws credentials
        credentials = aws_hook.get_credentials()
        
        # Instantiating the Redshift Hook
        redshift = PostgresHook(postgres_con_id=self.redshift_conn_id)
        
        self.log.info('Cleaning data on redshift destination table.')
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Copyng data to Redshift')
        
        # s3_key is the name of the file in the bucket
        rendered_key = self.s3_key.format(**context) 
        
        # Creating the path of the file in s3 bucket
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Creating the SQL statement for query execution
        sql_query = StageToRedshiftOperator.sql_statement.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.extra_params
        )
        
        print(sql_query)
        
        # Executing SQL query to populate the table
        redshift.run(sql_query)            
        
        self.log.info('Data inserted in redshift with success.')
        
#         self.log.info('StageToRedshiftOperator not implemented yet')
