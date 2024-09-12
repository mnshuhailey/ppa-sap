from dagster import resource
import pyodbc

@resource
def sqlserver_db_resource(context):
    # conn_str = (
    #     "DRIVER={ODBC Driver 17 for SQL Server};"
    #     "SERVER=10.10.1.199;"
    #     "DATABASE=PPA;"
    #     "UID=noor.shuhailey;"
    #     "PWD=Lzs.user831;"
    #     "Trust_Connection=yes;"
    # )
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.0.7;"
        "DATABASE=Test;"
        "UID=sa;"
        "PWD=123qwe;"
        "Trust_Connection=yes;"
    )
    try:
        connection = pyodbc.connect(conn_str)
        return connection
    except pyodbc.Error as ex:
        context.log.error(f"SQL Server connection failed: {ex}")
        raise
