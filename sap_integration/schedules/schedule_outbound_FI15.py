from dagster import schedule
from sap_integration.jobs import read_outbound_FI15_and_update_table_job

@schedule(cron_schedule="0 * * * *", job=read_outbound_FI15_and_update_table_job, execution_timezone="UTC")
def schedules_outbound_FI15(_context):
    return {}
