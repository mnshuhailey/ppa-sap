from dagster import job, op
from sap_integration.resources import sqlserver_db_resource, postgres_db_resource, sftp
from sap_integration.ops.generate_FI07 import generate_FI07
from sap_integration.ops.generate_FI09 import generate_FI09
from sap_integration.ops.generate_FI10 import generate_FI10
from sap_integration.ops.generate_FI15 import generate_FI15
from sap_integration.ops.read_update_FI16 import read_update_FI16
from sap_integration.ops.read_update_FI21 import read_update_FI21
from sap_integration.ops.read_update_FI09 import read_update_FI09
from sap_integration.ops.read_update_FI10 import read_update_FI10
from sap_integration.ops.read_update_FI15 import read_update_FI15
import os
from datetime import datetime
import pysftp
import traceback

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

@op
def get_fi15_filename(context):
    execution_time = datetime.now()
    filename = f"FI15_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")
    return filename

# Op to write to flat file
@op
def write_to_flatfile_file(context, formatted_lines, filename):
    if not formatted_lines:
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
def push_to_sftp(context, local_path, remote_folder):
    sftp_host = "192.168.10.176"
    sftp_username = "shuhailey"
    sftp_password = "Lzs.user831"  # Use environment variables instead for production security
    remote_path = "/home/shuhailey/sftpfile"
    remote_directory = os.path.dirname(remote_path)

    # Disable host key checking (for testing purposes only)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # This disables host key verification

    try:
        with pysftp.Connection(
                sftp_host,
                username=sftp_username,
                password=sftp_password,
                cnopts=cnopts
        ) as sftp:
            # Check if the remote directory exists
            if not sftp.exists(remote_directory):
                context.log.error(f"Remote directory {remote_directory} does not exist.")
                return

            # Check if the directory is writable by trying to change to it
            try:
                sftp.chdir(remote_directory)
                context.log.info(f"Changed directory to {remote_directory}.")
            except Exception as perm_error:
                context.log.error(f"Cannot access remote directory {remote_directory}: {perm_error}")
                return

            # Attempt to upload the file
            sftp.put(local_path, remote_path)
            context.log.info(f"File {local_path} uploaded to SFTP server at {remote_path}")

    except Exception as e:
        error_message = f"Failed to upload file to SFTP server at {sftp_host}. Exception: {str(e)}"
        stack_trace = traceback.format_exc()
        context.log.error(error_message)
        context.log.error(f"Stack Trace: {stack_trace}")

# Job for FI07
@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def generate_FI07_and_push_flatfile_job():
    filename = get_fi07_filename()
    formatted_lines = generate_FI07()
    # write_to_flatfile_file(formatted_lines, filename)
    local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI09
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
def generate_FI09_and_push_flatfile_job():
    # filename = get_fi09_filename()
    generate_FI09()
    # local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI10
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
def generate_FI10_and_push_flatfile_job():
    # filename = get_fi10_filename()
    generate_FI10()
    # local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI15
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
def generate_FI15_and_push_flatfile_job():
    # filename = get_fi15_filename()
    generate_FI15()
    # local_path = write_to_flatfile_file(formatted_lines, filename)
    # push_to_sftp(local_path)

# Job for FI16
# @job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "postgres_db": postgres_db_resource, "sftp": sftp})
def read_FI16_and_update_table_job():
    read_update_FI16()

# Job for FI21
# @job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "postgres_db": postgres_db_resource, "sftp": sftp})
def read_FI21_and_update_table_job():
    read_update_FI21()

# Job for Outbound
# @job(resource_defs={"sqlserver_db": sqlserver_db_resource, "sftp": sftp})
@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "postgres_db": postgres_db_resource, "sftp": sftp})
def read_outbound_FI09_and_update_table_job():
    read_update_FI09()

@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "postgres_db": postgres_db_resource, "sftp": sftp})
def read_outbound_FI10_and_update_table_job():
    read_update_FI10()

@job(resource_defs={"sqlserver_db": sqlserver_db_resource, "postgres_db": postgres_db_resource, "sftp": sftp})
def read_outbound_FI15_and_update_table_job():
    read_update_FI15()
