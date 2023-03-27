from sql_queries import *
import psycopg2
import configparser

# Importing setting information for creating the data warehouse
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_HOST=config.get('CLUSTER', 'HOST')
DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))
ARN=config.get('IAM_ROLE', 'ARN')
REGION=config.get('S3', 'REGION')

# Tables to be created
create_tables = [create_staging_imigration, create_staging_sas_information, create_dim_country, create_dim_state, create_dim_modal, create_dim_visa_motive, create_dim_port, create_dim_imigrant, create_fact_imigration]

# Parameters for importing the staging tables to Redshift
staging_tables_csv = [{
                    'table_name':'staging_sas_information',
                    'origin_path':'s3://nogfel-imigration/sas_data',
                    'delimiter':'|'
                }]

staging_table_parquet = {
                    'table_name':'staging_imigration',
                    'column_names':'"cicid", "i94yr", "i94mon", "i94cit", "i94res", "i94port", "arrdate", "i94mode", "i94addr", "depdate", "i94bir", "i94visa", "count", "dtadfile", "visapost", "occup", "entdepa", "entdepd", "entdepu", "matflag", "biryear", "dtaddto", "gender", "insnum","airline", "admnum", "fltno", "visatype"',
                    'origin_path':'s3://nogfel-imigration/imigration_data',
                }


# SQL query statement to import data to Redshift
sql_statement_staging_csv = """
COPY {} FROM '{}'
credentials 'aws_iam_role={}'
region '{}'
delimiter '{}' EMPTYASNULL CSV NULL AS '\\0'
IGNOREHEADER 1;
"""

sql_statement_staging_parquet = """
COPY {} ({}) FROM '{}'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
"""
    
# Parameters for formating dimension SQL queries
dim_tables =[{
                'table_name':'dim_country',
                'columns':'id, country',
                'query':load_dim_country
            },
            {
                'table_name':'dim_state',
                'columns':'id, state',
                'query':load_dim_state
            },
            {
                'table_name':'dim_modal',
                'columns':'id_modal, modal',
                'query':load_dim_modal
            },
            {
                'table_name':'dim_visa_motive',
                'columns':'visa_motive_id, motive',
                'query':load_dim_visa_motive
            },
            {
                'table_name':'dim_port',
                'columns':'port_modal_id, port_id, modal_id, location_city, location_state_id',
                'query':load_dim_port
            },
            {
                'table_name':'dim_imigrant',
                'columns':'imigrant_id, gender, occupation, birth_year, admission_number, birth_country_id, state_residence_id, visa_motive_id, visa_issued_place, visa_type',
                'query':load_dim_imigrant
            }]

# Parameters for formating the fact SQL query
fact_tables =[{
                'table_name':'fact_imigration',
                'columns':'port_modal_id, imigrant_id, id_port_arrival_us, id_modal, count, flight_number',
                'query':load_fact_imigration
            }]

# Query template to be use to load data into fact and dimensions table
loading_query = """
INSERT INTO {} ({}) 
{}
"""

def main():
    '''This function is responsible for performing the whole process of creating the tables (staings, dimensions and fact) and load them. This is done by performing a connection to Redshift using psycopg2, using the queries found in the `sql_queries.py` file and the support objects displayed previously in this file, such as: the lists of dictionaries containing the tables informations, the queries templates to be used to insert data and the imports from `dwh.cfg` with the proper dw configurations.'''

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    conn.autocommit = True
    cur = conn.cursor()

    print('Creating tables')
    print('\n')
    # Creating tables
    for query in create_tables:
        cur.execute(query=query)
        

    # Loading staging tables from .csv file
    for table in staging_tables_csv:
        print('Truncating {} table'.format(table['table_name']))
        cur.execute('TRUNCATE TABLE {}'.format(table['table_name']))

        print('Generating {} SQL query from .csv file'.format(table['table_name']))
        staging_query_csv = sql_statement_staging_csv.format(table['table_name'], table['origin_path'], ARN, REGION,table['delimiter'])

        print('Executing {} query'.format(table['table_name']))
        cur.execute(query=staging_query_csv)

        print('`{}` loaded with success \n'.format(table['table_name']))

    # Loadging staging table from parquet file
    print('Truncating staging_imigration table')
    cur.execute('TRUNCATE TABLE staging_imigration')

    print('Generating staging_imigration SQL query from .parquet file')
    staging_query_parquet = sql_statement_staging_parquet.format(
        staging_table_parquet['table_name'], 
        staging_table_parquet['column_names'], 
        staging_table_parquet['origin_path'], 
        ARN
    )

    print('Executing staging_imigration query')
    cur.execute(query=staging_query_parquet)

    print('`staging_imigration` loaded with success \n')

    # Loading dimension tables
    for table in dim_tables:
        print('Truncating {} table'.format(table['table_name']))
        cur.execute('TRUNCATE TABLE {}'.format(table['table_name']))

        print('Generating {} SQL query'.format(table['table_name']))
        load_dimension_query = loading_query.format(table['table_name'], table['columns'], table['query'])

        print('Executing {} query \n'.format(table['table_name']))
        cur.execute(query=load_dimension_query)

    # Loading fact table
    for table in fact_tables:
        print('Truncating {} table'.format(table['table_name']))
        cur.execute('TRUNCATE TABLE {}'.format(table['table_name']))

        print('Generating {} SQL query'.format(table['table_name']))
        loading_fact_query = loading_query.format(table['table_name'], table['columns'], table['query'])

        print('Executing {} query \n'.format(table['table_name']))
        cur.execute(query=loading_fact_query)

    print('All tables created and loaded with success.')

if __name__ == "__main__":
    main()