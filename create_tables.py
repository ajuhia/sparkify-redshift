import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops all the tables (if exist in DB) mentioned
    in the list drop_table_queries imported from sql_queries.py.
    Input parameters:
        - cur : cursor to the database in redshift.
        - conn: connection to the database in redshift.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates all the tables (if not exist in DB) mentioned
    in the list create_table_queries imported from sql_queries.py.
    Input parameters:
        - cur : cursor to the database in redshift.
        - conn: connection to the database in redshift.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function fetches the configuration parameters from dhw.cfg file
    using ConfigParser, connects to db in the redshift cluster using the
    above fetched config details, fetches the db cursor and makes calls 
    to drop_tables() and create_tables() to drop existing and create new
    tables in DB. And finally the connection is closed.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        #connect to the db
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        #Drop existing tables and create new ones
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        #close db connection
        conn.close()
        
    except psycopg2.Error as e:
        print("Erorr while connecting or inserting data in DB in redshift"+str(e))


if __name__ == "__main__":
    main()