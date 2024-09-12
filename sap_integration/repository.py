from dagster import repository
from sap_integration.jobs import generate_and_push_flatfile_job
from sap_integration.schedules import hourly_schedule
from sap_integration.sensors import new_data_sensor

@repository
def sap_integration_repo():
    return [generate_and_push_flatfile_job, hourly_schedule, new_data_sensor]
