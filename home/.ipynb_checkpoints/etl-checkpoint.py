import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads the staging tables in
    redshift db by copying data from S3 buckets,
    by processing queries in copy_table_queries
    list of sql_queries.py.
    Input parameters:
        - cur: cursor to db in redshift
        - conn:connection to db in redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts processed data into staging
    tables of a database in redshift by processing queries in 
    insert_table_queries list of sql_queries.py.
    Input parameters:
        - cur: cursor to db in redshift
        - conn:connection to db in redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function fetches the configuration parameters from dhw.cfg file
    using ConfigParser, connects to db in the redshift cluster using the
    above fetched config details, fetches the db cursor and makes calls 
    to load_staging_tables() and insert_tables() to load staging tables 
    from S3 and insert processed data final tables in DB.
    And finally the connection is closed.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        #connect to db
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        
        cur = conn.cursor()
        
        
        #insert data into tables
        load_staging_tables(cur, conn)
        
        insert_tables(cur, conn)
        
        #close db connection
        conn.close()
    
    except psycopg2.Error as e:
        print("Erorr while connecting or inserting data in DB in redshift"+str(e))


if __name__ == "__main__":
    main()