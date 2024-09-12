from dagster import job, op
from datetime import datetime
from sap_integration.resources import sqlserver_db_resource
import os
# import pysftp  # Commented out since SFTP is not configured yet

@op(required_resource_keys={"sqlserver_db"})
def extract_data_from_sqlserver(context):
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()
    
    query = "SELECT * FROM dbo.asnaf_transformed_v4"
    cursor.execute(query)
    data = cursor.fetchall()

    if not data:  # Check if data is empty
        context.log.info("No data found in the SQL Server table. Skipping file generation.")
        return None

    formatted_lines = []

    # Add the header
    date_created = "2024090911250000"  # Fixed datetime value in YYYYMMDDHHMMSSSS format
    header = f"0|FI07|{date_created}|PPA||{len(data)}"
    formatted_lines.append(header)

    for row in data:
        line1 = f"1|ZB90||2|{row.Name}|{row.Gender}||{row.IdentificationNumIC}|{row.AsnafID}"
        line2 = f"2|{row.Street1}|{row.Street2}|{row.Street3}|||{row.City}|{row.Postcode}|{row.State}|{row.Country}|EN|{row.TelephoneNoHome}||{row.MobilePhoneNum}|{row.Emel}"
        line3 = f"3|0001|MY||{row.BankAccountNum}|"
        line4 = f"4|MY4|{row.IdentificationNumIC}"
        line5 = "5|001|5000002|V090||X|CDEFJTV|||X"

        formatted_lines.extend([line1, line2, line3, line4, line5])

    context.log.info(f"Extracted and formatted {len(data)} rows of data.")
    return formatted_lines

@op
def write_to_flatfile_file(context, formatted_lines):
    if formatted_lines is None:
        context.log.info("No data to write. Skipping file generation.")
        return None

    folder_path = "./ppa-flatefile"

    # Create the folder if it does not exist
    os.makedirs(folder_path, exist_ok=True)

    # Fixed filename format
    filename = "FI07_20240909112500.txt"  
    local_path = os.path.join(folder_path, filename)

    with open(local_path, "w") as file:
        for line in formatted_lines:
            file.write(line + "\n")
    context.log.info(f"Written data to {local_path}")
    return local_path

# Commented out push_to_sftp function for now
# @op
# def push_to_sftp(context, local_path):
#     sftp_host = "your_sftp_host"
#     sftp_username = "your_sftp_username"
#     sftp_password = "your_sftp_password"
#     remote_path = f"/remote/path/{os.path.basename(local_path)}"
# 
#     with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password) as sftp:
#         sftp.put(local_path, remote_path)
#     
#     context.log.info(f"File {local_path} uploaded to SFTP server at {remote_path}")

@job(resource_defs={"sqlserver_db": sqlserver_db_resource})
def generate_and_push_flatfile_job():
    # Extract data from SQL Server
    formatted_lines = extract_data_from_sqlserver()
    
    # Write to flat file only if data exists
    if formatted_lines is not None:
        local_path = write_to_flatfile_file(formatted_lines)
        # push_to_sftp(local_path)  # Commented out the SFTP function call for now
