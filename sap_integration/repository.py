from dagster import repository
from sap_integration.jobs import generate_FI07_and_push_flatfile_job
from sap_integration.jobs import generate_FI09_and_push_flatfile_job
from sap_integration.jobs import generate_FI10_and_push_flatfile_job
from sap_integration.jobs import read_FI16_and_update_table_job
from sap_integration.schedules.schedule_FI07 import schedules_FI07
from sap_integration.schedules.schedule_FI09 import schedules_FI09
from sap_integration.schedules.schedule_FI10 import schedules_FI10
from sap_integration.schedules.schedule_FI16 import schedules_FI16

@repository
def sap_integration_FI07_repo():
    return [generate_FI07_and_push_flatfile_job, schedules_FI07]

@repository
def sap_integration_FI09_repo():
    return [generate_FI09_and_push_flatfile_job, schedules_FI09]

@repository
def sap_integration_FI10_repo():
    return [generate_FI10_and_push_flatfile_job, schedules_FI10]

# @repository
# def read_outbound_FI16_repo():
#     return [read_FI16_and_update_table_job, schedules_FI16]
