from dagster import schedule
from sap_integration.jobs import generate_and_push_flatfile_job

@schedule(cron_schedule="0 * * * *", job=generate_and_push_flatfile_job, execution_timezone="UTC")
def hourly_schedule(_context):
    return {}
