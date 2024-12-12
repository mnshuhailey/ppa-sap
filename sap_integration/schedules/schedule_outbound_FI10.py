from dagster import schedule
from sap_integration.jobs import read_outbound_FI10_and_update_table_job

@schedule(cron_schedule="0 * * * *", job=read_outbound_FI10_and_update_table_job, execution_timezone="Asia/Kuala_Lumpur")
def schedules_outbound_FI10(_context):
    return {}
