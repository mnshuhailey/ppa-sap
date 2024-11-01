from dagster import op
from datetime import datetime
import os

# @op(required_resource_keys={"sqlserver_db", "sftp"})
@op(required_resource_keys={"sqlserver_db", "postgres_db"})
def read_update_FI16(context):
    # Establish SFTP connection (commented for temporary local testing)
    # sftp = context.resources.sftp
    # with sftp.open('/path/to/yourfile.txt') as file:
    #     data = file.read().decode('utf-8')

    # Define the local file path and filename
    local_path = "./ppa-flatefile/FI16_20241024180058.txt"
    filename = os.path.basename(local_path)

    # Check if file exists in the PostgreSQL table `sap_integration_log` with status 'Read'
    postgres_conn = context.resources.postgres_db
    with postgres_conn.cursor() as cursor:
        check_query = """
            SELECT 1 FROM public.sap_integration_log WHERE filename = %s AND status = 'Read'
        """
        cursor.execute(check_query, (filename,))
        result = cursor.fetchone()

    # If filename with status 'Read' exists, log a message and stop the function
    if result:
        context.log.info(f"File data for {filename} already updated inside SQL DB with status 'Read'.")
        return None

    # Proceed with file processing if filename does not exist with status 'Read'
    if not os.path.exists(local_path):
        context.log.error(f"File not found at path: {local_path}")
        return None  # or handle the error as needed

    # Read the content of the file
    with open(local_path, "r") as file:
        raw_data = file.read()

    # Split lines and separate header from data rows
    lines = raw_data.strip().split('\n')
    header = lines[0].split('|')  # Assuming the first line is the header
    data_rows = [line.split('|') for line in lines[1:] if line.startswith('1')]

    # Process header if needed
    header_info = {
        "record_type": header[0],
        "document_type": header[1],
        "timestamp": header[2]
        # Add any other header information as needed
    }

    # Insert file details into PostgreSQL `sap_integration_log` table
    with postgres_conn.cursor() as cursor:
        insert_query = """
            INSERT INTO public.sap_integration_log (file_type, filename, raw_data, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI16', filename, raw_data, 'Read'))
        postgres_conn.commit()
    context.log.info(f"Inserted log entry for file: {filename}")

    # Connect to SQL Server
    sqlserver_conn = context.resources.sqlserver_db
    cursor = sqlserver_conn.cursor()

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

    # Commit SQL Server transaction
    sqlserver_conn.commit()

    # Log completion message
    context.log.info(
        f"Data update completed for document type: {header_info['document_type']} at {header_info['timestamp']}"
    )
