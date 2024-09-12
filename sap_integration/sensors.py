from dagster import sensor, RunRequest
from sap_integration.jobs import generate_and_push_flatfile_job

@sensor(job=generate_and_push_flatfile_job)
def new_data_sensor(context):
    # Logic to check for new data, e.g., query SQL server or check file directory
    # If the condition is met, yield RunRequest
    condition_met = True  # Replace with your actual condition logic
    if condition_met:
        yield RunRequest(run_key="new_data")
