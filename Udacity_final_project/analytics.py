from sql_queries import *

question_queries = {"How many people imigrated to US in April 2016":total_imigration,
                    "Which modal is the most common used for going to US?":imigrant_modal_transp,
                    "What is the most common visa type for aerial arrivals?":visa_type_aerial,
                    "Which is the most common place where people arrive using aerial modal and comes for pleasure?":arrival_aerials_pleasure}

def connect_redshift():
    import psycopg2
    import configparser

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_HOST=config.get('CLUSTER', 'HOST')
    DWH_DB_NAME=config.get('CLUSTER', 'DB_NAME')
    DB_UDWH_SER=config.get('CLUSTER', 'DB_USER')
    DB_PASSWDWH_ORD=config.get('CLUSTER', 'DB_PASSWORD')
    DWH_DB_PORT=int(config.get('CLUSTER', 'DB_PORT'))
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST,DWH_DB_NAME, DB_UDWH_SER, DB_PASSWDWH_ORD, DWH_DB_PORT))
    conn.autocommit = True
    cur = conn.cursor()

    return cur

def run_analysis(cur, query):
    cur.execute(query)
    response = cur.fetchall()

    for item in response:
        print(item)


def main():
    for question, query in question_queries.items():
        print(question)
        run_analysis(
            cur=connect_redshift(),
            query=query
        )

if __name__ == "__main__":
    main()
