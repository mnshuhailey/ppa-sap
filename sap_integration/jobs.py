from dagster import job, op
from sap_integration.resources import sqlserver_db_resource
from sap_integration.ops.generate_FI07 import generate_FI07
from sap_integration.ops.generate_FI09 import generate_FI09
from sap_integration.ops.generate_FI10 import generate_FI10
from sap_integration.ops.read_update_FI16 import read_update_FI16
import os
from datetime import datetime
import pysftp

# Op to dynamically generate the filename for FI07
@op
def get_fi07_filename(context):
    execution_time = datetime.now()
    filename = f"FI07_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")
    return filename

# Op to dynamically generate the filename for FI09
@op
def get_fi09_filename(context):
    execution_time = datetime.now()
    filename = f"FI09_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")
    return filename

# Op to dynamically generate the filename for FI10
@op
def get_fi10_filename(context):
    execution_time = datetime.now()
    filename = f"FI10_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")
    return filename

# Op to write to flat file
@op
def write_to_flatfile_file(context, formatted_lines, filename):
    if formatted_lines is None:
        context.log.info("No data to write. Skipping file generation.")
        return None

    folder_path = "./ppa-flatefile"

    # Create the folder if it does not exist
    os.makedirs(folder_path, exist_ok=True)

    local_path = os.path.join(folder_path, filename)

    with open(local_path, "w") as file:
        for line in formatted_lines:
            file.write(line + "\n")
    
    context.log.info(f"Written data to {local_path}")
    return local_path

# Commented out push_to_sftp function for now
@op
def push_to_sftp(context, local_path):
    sftp_host = "eu-central-1.sftpcloud.io"
    sftp_username = "dd071a2846a14e009465e8bf4bf79155"
    sftp_password = "t2ARiDvJ94l9kZ5s5KrRI1h05KWACVp1"  # Remove this if using key authentication
    remote_path = ""

    # If using public key authentication, include `private_key` and `private_key_pass`
    # with pysftp.Connection(
    #         sftp_host,
    #         username=sftp_username,
    #         password=sftp_password,  # Remove or replace with `private_key` if needed
    # ) as sftp:
    #     sftp.put(local_path, remote_path)

    # Establish SFTP connection using only username and password
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
            sftp_host,
            username=sftp_username,
            password=sftp_password,
            cnopts=cnopts
    ) as sftp:
        sftp.put(local_path, remote_path)

    context.log.info(f"File {local_path} uploaded to SFTP server at {remote_path}")

# Job for FI07
@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def generate_FI07_and_push_flatfile_job():
    filename = get_fi07_filename()
    formatted_lines = generate_FI07()
    # write_to_flatfile_file(formatted_lines, filename)
    local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI09
@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def generate_FI09_and_push_flatfile_job():
    filename = get_fi09_filename()
    formatted_lines = generate_FI09()
    local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI10
@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def generate_FI10_and_push_flatfile_job():
    filename = get_fi10_filename()
    formatted_lines = generate_FI10()
    local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI16
@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def read_FI16_and_update_table_job():
    read_update_FI16()
