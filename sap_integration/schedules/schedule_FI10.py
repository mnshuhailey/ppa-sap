from dagster import schedule
from sap_integration.jobs import generate_FI10_and_push_flatfile_job

@schedule(cron_schedule="0 * * * *", job=generate_FI10_and_push_flatfile_job, execution_timezone="Asia/Kuala_Lumpur")
def schedules_FI10(_context):
    return {}
