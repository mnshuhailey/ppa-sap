from dagster import op
from datetime import datetime
import pysftp
import os

# @op(required_resource_keys={"sqlserver_db", "sftp"})
@op(required_resource_keys={"sqlserver_db"})
def read_update_FI16(context):
    # Establish SFTP connection (commented for temporary local testing)
    # sftp = context.resources.sftp
    # with sftp.open('/path/to/yourfile.txt') as file:
    #     data = file.read().decode('utf-8')

    # For temporary, read the file's content from the local path
    # with open('../ppa-flatefile/FI16_20241024180058.txt', 'r') as file:
    #     data = file.read()

    local_path = "./ppa-flatefile/FI16_20241024180058.txt"
    if not os.path.exists(local_path):
        context.log.error(f"File not found at path: {local_path}")
        return None  # or handle the error as needed

    with open(local_path, "r") as file:
        data = file.read()

    # Split lines and separate header from data rows
    lines = data.strip().split('\n')
    header = lines[0].split('|')  # Assuming the first line is the header
    data_rows = [line.split('|') for line in lines[1:] if line.startswith('1')]

    # Process header if needed
    header_info = {
        "record_type": header[0],
        "document_type": header[1],
        "timestamp": header[2]
        # Add any other header information as needed
    }

    # Connect to SQL Server
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    # Loop through data rows and perform updates based on status
    for row in data_rows:
        # Extract fields based on the structure provided
        record_indicator, company_code, payment_advice_name, status, pa_message, pad_id = row

        # Log extracted values
        context.log.info(f"Record Indicator: {record_indicator}, Company Code: {company_code}, Payment Advice Name: {payment_advice_name}, Status: {status}, PA Message: {pa_message}, PAD ID: {pad_id}")

        # Update if the status is "PRINTED"
        if status == "PRINTED":
            date_str = pad_id.split('-')[-1]
            ad_printed_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")

            query = """
            UPDATE dbo.PaymentAdvice
            SET Synced_status = 'Synced', ad_saprefno = ?, status = ?, PA_message = ?, ad_PrintedDate = ?
            WHERE PaymentAdviceName = ?
            """
            cursor.execute(query, (header[2], status, pa_message, ad_printed_date, payment_advice_name))
            context.log.info(f"Updated PRINTED record for Payment Advice Name: {payment_advice_name}")

        # Update if the status is "VOIDED"
        elif status == "VOIDED":
            date_str = pad_id.split('-')[-1]
            ad_voiddate = datetime.strptime(date_str, "%Y%m%d").strftime("%Y%m%d")

            query = """
            UPDATE dbo.PaymentAdvice
            SET Synced_status = 'Synced', ad_saprefno = ?, status = ?, PA_message = ?, ad_voiddate = ?
            WHERE PaymentAdviceName = ?
            """
            cursor.execute(query, (header[2], status, pa_message, ad_voiddate, payment_advice_name))
            context.log.info(f"Updated VOIDED record for Payment Advice Name: {payment_advice_name}")

    # Commit transaction
    conn.commit()

    # Log completion message
    context.log.info(
        f"Data update completed for document type: {header_info['document_type']} at {header_info['timestamp']}")
