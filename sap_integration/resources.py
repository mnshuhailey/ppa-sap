from dagster import resource
import pyodbc
import pysftp
import psycopg2

# PostgreSQL resource configuration
@resource
def postgres_db_resource(context):
    return psycopg2.connect(
        host="192.168.10.177",
        port=5432,
        user="postgres",
        password="secret123",
        database="postgres_db",
    )

@resource
def sqlserver_db_resource(context):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=10.10.1.199;"
        "DATABASE=PPA;"
        "UID=noor.shuhailey;"
        "PWD=Lzs.user831;"
        "Trust_Connection=yes;"
    )
    # conn_str = (
    #     "DRIVER={ODBC Driver 17 for SQL Server};"
    #     "SERVER=192.168.0.14;"
    #     "DATABASE=Test_Internal;"
    #     "UID=sa;"
    #     "PWD=123qwe;"
    #     "Trust_Connection=yes;"
    # )
    try:
        connection = pyodbc.connect(conn_str)
        return connection
    except pyodbc.Error as ex:
        context.log.error(f"SQL Server connection failed: {ex}")
        raise

# @resource
# def sftp():
#     # Adjust to your SFTP credentials and server details
#     sftp_credentials = {
#         'host': 'sftp_host',
#         'username': 'your_username',
#         'private_key': 'path/to/private_key'
#     }
#
#     return pysftp.Connection(**sftp_credentials)