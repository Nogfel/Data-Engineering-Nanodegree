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
create_tables = [create_staging_airport_codes, create_staging_imigration, create_staging_sas_information, create_dim_country, create_dim_state, create_dim_modal, create_dim_visa_motive, create_dim_port, create_dim_imigrant, create_fact_imigration]

# Parameters for importing the staging tables to Redshift
staging_tables = [{
                    'table_name':'staging_airport_codes',
                    'origin_path':'s3://nogfel-imigration1/airport_data',
                    'delimiter':','
                },
                {
                    'table_name':'staging_imigration',
                    'origin_path':'s3://nogfel-imigration1/imigration_data',
                    'delimiter':','
                },
                {
                    'table_name':'staging_sas_information',
                    'origin_path':'s3://nogfel-imigration1/sas_data',
                    'delimiter':'|'
                }]


# SQL query statement to import data to Redshift
sql_statement_staging = """
COPY {} FROM '{}'
credentials 'aws_iam_role={}'
region '{}'
delimiter '{}' EMPTYASNULL CSV NULL AS '\\0'
IGNOREHEADER 1;
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
                'columns':'port_modal_id, port_id, modal_id, port_type, port_name, port_country, port_city',
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
                'columns':'imigrant_id, id_port_arrival_us, count, flight_number',
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
        

    # # Loading staging tables
    for table in staging_tables:
        print('Truncating {} table'.format(table['table_name']))
        cur.execute('TRUNCATE TABLE {}'.format(table['table_name']))

        print('Generating {} SQL query'.format(table['table_name']))
        staging_query = sql_statement_staging.format(table['table_name'], table['origin_path'], ARN, REGION,table['delimiter'])

        print('Executing {} query'.format(table['table_name']))
        cur.execute(query=staging_query)

        print('`{}` loaded with success \n'.format(table['table_name']))

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