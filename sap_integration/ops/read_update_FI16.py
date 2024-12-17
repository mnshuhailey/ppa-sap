from dagster import op
from datetime import datetime, timedelta
import os

@op(required_resource_keys={"sqlserver_db", "sftp"})
def read_update_FI16(context):
    # Define file path and extract filename
    REMOTE_FOLDER = "FI16/Processed"

    # Get the SFTP connection from the resource
    sftp_conn = context.resources.sftp

    sqlserver_conn = context.resources.sqlserver_db

    try:
        # Check if the directory exists on the SFTP server
        if not sftp_conn.exists(REMOTE_FOLDER):
            context.log.error(f"SFTP directory not found: {REMOTE_FOLDER}")
            return

        # Define start date and current date
        start_date = datetime.strptime("2024-11-26", "%Y-%m-%d")
        current_date = datetime.now()

        # Iterate over each date in the range
        delta = (current_date - start_date).days + 1
        for day_offset in range(delta):
            processing_date = start_date + timedelta(days=day_offset)
            date_str = processing_date.strftime("%Y%m%d")
            file_prefix = f"FI16_{date_str}"

            # List files in the specified remote folder
            files = sftp_conn.listdir(REMOTE_FOLDER)

            # Filter files matching the required format
            matching_files = [f for f in files if f.startswith(file_prefix) and f.endswith(".txt")]

            if not matching_files:
                context.log.info(f"No files matching the required format for {processing_date.strftime('%Y-%m-%d')}.")
                continue

            context.log.info(f"Found {len(matching_files)} matching file(s) for {processing_date.strftime('%Y-%m-%d')}: {matching_files}")

            for file_name in matching_files:
                # Check if the file has already been processed
                if file_already_processed(context, sqlserver_conn, file_name):
                    context.log.info(f"File data for {file_name} already updated in SQL Server with status 'Read'. Skipping.")
                    continue

                # Read the contents of the file
                file_path = f"{REMOTE_FOLDER}/{file_name}"
                with sftp_conn.open(file_path, "r") as file:
                    data_raw = file.read().decode("utf-8")

                # Log the raw data
                context.log.info(f"Raw data from the file ({file_name}):\n{data_raw}")

                # Parse header and data rows
                lines = data_raw.strip().split("\n")
                header = lines[0].split("|")
                data_rows = [line.split("|") for line in lines[1:] if line.strip()]

                # Log header and data rows
                context.log.info(f"Header: {header}")
                context.log.info(f"Data rows: {data_rows}")

                # Check if data rows exist
                if not data_rows:
                    context.log.error(f"No data rows found in the file: {file_name}")
                    continue

                # Process the header
                header_info = {
                    "record": header[0],
                    "record_type": header[1],
                    "timestamp": header[2],
                    "document_type": header[3]
                }

                # Log header information
                context.log.info(f"Parsed header info: {header_info}")

                # Process each row in the file
                for row in data_rows:
                    if len(row) != 6:
                        context.log.error(f"Invalid row format: {row}. Skipping.")
                        continue

                    # Process each data row
                    process_data_row(context, sqlserver_conn, header_info["timestamp"], row)

                # Insert a log entry for the file
                insert_file_log(sqlserver_conn, header_info['record_type'], file_name, data_raw)
                context.log.info(f"Inserted log entry for file: {file_name}")

        # Commit the updates after processing all files
        sqlserver_conn.commit()
        context.log.info(f"Completed processing files from {start_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}.")

    except Exception as e:
        context.log.error(f"Error during SFTP processing: {e}")
    finally:
        # Close the SFTP connection
        sftp_conn.close()

def file_already_processed(context, sqlserver_conn, filename):
    """Check if the file has already been processed in SQL Server."""
    with sqlserver_conn.cursor() as cursor:
        check_query = """
            SELECT 1 FROM dbo.SAP_Integration_Outbound WHERE filename = ? AND status = 'Read'
        """
        cursor.execute(check_query, (filename,))
        return cursor.fetchone() is not None

def insert_file_log(sqlserver_conn, file_type, filename, data_raw):
    """Insert a log entry for the file in the sap_integration_log table."""
    with sqlserver_conn.cursor() as cursor:
        insert_query = """
            INSERT INTO dbo.SAP_Integration_Outbound (file_type, filename, data_raw, status, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, (file_type, filename, data_raw, 'Read'))
        sqlserver_conn.commit()

def format_payment_advice_name(context, payment_advice_name):
    """Formats the ad_synced_date string to the desired format."""
    if not payment_advice_name :
        context.log.info("Empty PaymentAdviceName.")
        return

    # Split the payment advice string by '/'
    parts = payment_advice_name.split("/")
    if len(parts) != 3:
        context.log.error("Invalid PaymentAdviceName format. Expected format: 'PA/YYYY/NNNNNNNN'.")
        return
    
    prefix = payment_advice_name.split("/")[0]
    year = payment_advice_name.split("/")[1]
    number = payment_advice_name.split("/")[2]

    return f"{prefix}-{year}-{number}"

def process_data_row(context, sqlserver_conn, timestamp, row):
    """Process a single row of data based on the status and perform updates accordingly."""
    record_indicator, company_code, payment_advice_name, status, pa_message, pad_id = row
    context.log.info(f"Processing Payment Advice: {payment_advice_name} with status: {status}")

    date_str = pad_id.split('-')[-1]
    payment_advice = format_payment_advice_name(context, payment_advice_name)
    
    # Check if PaymentAdviceName exists
    check_query = "SELECT COUNT(1) FROM dbo.PaymentAdvice WHERE PaymentAdviceName = ?"
    with sqlserver_conn.cursor() as cursor:
        cursor.execute(check_query, (payment_advice,))
        result = cursor.fetchone()
        if result[0] == 0:  # If no matching record exists
            context.log.error(f"PaymentAdviceName '{payment_advice}' does not exist in the database.")
            return  # Skip the update if no record is found
                
    try:
        if status == "PRINTED":
            ad_printed_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")
            update_payment_advice(context, sqlserver_conn, payment_advice, timestamp, status, pa_message, ad_printed_date, 'ad_PrintedDate')
            context.log.info(f"Updated PRINTED record for Payment Advice Name: {payment_advice}")
        elif status == "VOIDED":
            ad_voiddate = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")
            update_payment_advice(context, sqlserver_conn, payment_advice, timestamp, status, pa_message, ad_voiddate, 'ad_voiddate')
            context.log.info(f"Updated VOIDED record for Payment Advice Name: {payment_advice}")
    except ValueError as e:
        context.log.error(f"Date parsing error for PAD ID {pad_id} in row: {row}. Error: {e}")

def update_payment_advice(context, sqlserver_conn, payment_advice_name, timestamp, status, pa_message, date_value, date_field):
    """Update the PaymentAdvice table based on the provided details."""

    if not payment_advice_name:
        context.log.error("PaymentAdviceName must be provided.")
        return
    
    try:
        update_query = """
            UPDATE dbo.PaymentAdvice
            SET Synced_status = 'Synced', ad_saprefno = ?, status = ?, PA_message = ?, {date_field} = ?
            WHERE PaymentAdviceName = ?
        """
        parameters = (timestamp, status, pa_message, date_value, payment_advice_name)
        with sqlserver_conn.cursor() as cursor:
            cursor.execute(update_query, parameters)
            sqlserver_conn.commit()
            context.log.info(f"Updated PaymentAdvice: {payment_advice_name}.")

    except Exception as e:
        context.log.error(f"Error updating PaymentAdvice: {e}")

