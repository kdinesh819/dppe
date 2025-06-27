import pandas as pd
#import pyodbc
import pymysql
#import psycopg2
#from hdbcli import dbapi as hana
#psycopg2==2.9.10
#hdbcli==2.24.24
#dbapi==0.0.14
def fetch_data(db_type, host, port, user, password, database, query):
    try:
        #if db_type == 'mssql':
            #conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
            #conn = pyodbc.connect(conn_str)
        if db_type == 'mysql':
            conn = pymysql.connect(host=host, user=user, password=password, db=database)
        #elif db_type == 'postgresql':
        #    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=database)
        #elif db_type == 'hana':
        #    conn = hana.connect(address=host, port=int(port), user=user, password=password)
        else:
            raise ValueError("Unsupported DB type")

        df = pd.read_sql(query, conn)
        conn.close()
        return df

    except Exception as e:
        print(f"DB Fetch Error: {e}")
        return None
