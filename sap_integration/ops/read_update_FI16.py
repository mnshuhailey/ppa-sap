from dagster import op
from datetime import datetime

@op(required_resource_keys={"sqlserver_db"})
def read_update_FI16(context):
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    return data
