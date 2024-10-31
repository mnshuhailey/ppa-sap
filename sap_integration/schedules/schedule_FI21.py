from dagster import schedule
from sap_integration.jobs import read_FI21_and_update_table_job

@schedule(cron_schedule="0 * * * *", job=read_FI21_and_update_table_job, execution_timezone="UTC")
def schedules_FI21(_context):
    return {}
