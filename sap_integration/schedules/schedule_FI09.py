from dagster import schedule
from sap_integration.jobs import generate_FI09_and_push_flatfile_job

@schedule(cron_schedule="0 * * * *", job=generate_FI09_and_push_flatfile_job, execution_timezone="UTC")
def schedules_FI09(_context):
    return {}
