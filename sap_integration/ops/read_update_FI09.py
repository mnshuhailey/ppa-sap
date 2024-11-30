from dagster import op
import os
from datetime import datetime

@op(required_resource_keys={"sqlserver_db", "sftp"})
def read_update_FI09(context):
    # Define the SFTP path
    sftp_path = "FI09/Status/Read/"

    # Get the SFTP connection from the resource
    sftp_conn = context.resources.sftp

    try:
        # Check if the directory exists on the SFTP server
        if not sftp_conn.exists(sftp_path):
            context.log.error(f"SFTP directory not found: {sftp_path}")
            return

        # Get the current date
        today = datetime.now().date()

        # Iterate through each file in the SFTP directory
        for filename in sftp_conn.listdir(sftp_path):
            file_path = os.path.join(sftp_path, filename)

            # Skip if it's not a file
            if not sftp_conn.isfile(file_path):
                continue

            # Get the file's metadata
            file_stat = sftp_conn.lstat(file_path)
            file_mod_time = datetime.fromtimestamp(file_stat.st_mtime).date()

            # Check if the file was modified today
            if file_mod_time != today:
                continue

            # Log the file being processed
            context.log.info(f"Processing today's file: {filename}")

            # Check if the file has already been processed
            # sqlserver_conn = context.resources.sqlserver_db
            # if file_already_processed(context, sqlserver_conn, filename):
            #     context.log.info(f"File data for {filename} already updated in SQL Server with status 'Read'.")
            #     continue

            # Read the file content from SFTP
            with sftp_conn.open(file_path, "r") as file:
                data_raw = file.read()

            # Parse header and data rows
            lines = data_raw.strip().split('\n')
            header = lines[0].split('|')
            data_rows = [line.split('|') for line in lines[1:] if line.strip()]

            # Log data_rows
            context.log.info(f"Data rows in file {filename}: {data_rows}")

            # Check if data_rows exists
            if not data_rows:
                context.log.error(f"No data rows found in the file: {filename}")
                continue

            header_info = {
                "record_type": header[0],
                "document_type": header[1],
                "timestamp": header[2]
            }

            # Insert a log entry for the file
            # insert_file_log(sqlserver_conn, header_info['record_type'], filename, data_raw)
            context.log.info(f"Inserted log entry for file: {filename}")

            # Process each row in the file
            # for row in data_rows:
            #     if len(row) != 3:
            #         context.log.error(f"Invalid row format: {row}. Skipping.")
            #         continue

            #     # Process each data row
            #     process_data_row(context, sqlserver_conn, row, header_info['record_type'])

            # Commit the updates
            # sqlserver_conn.commit()
            # context.log.info(f"Completed processing file: {filename}")

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

def process_data_row(context, sqlserver_conn, row, file_type):
    """Process a single row of data based on the status and perform updates accordingly."""
    payment_advice_name, ad_synced_date, status_pa_message = row
    context.log.info(f"Processing Payment Advice: {payment_advice_name} with status: {status_pa_message}")

    try:
        if file_type in ["FI09", "FI10"]:
            payment_advice = format_payment_advice_name(context, payment_advice_name)

            # Check if PaymentAdviceName exists
            check_query = "SELECT COUNT(1) FROM dbo.PaymentAdvice WHERE PaymentAdviceName = ?"
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(check_query, (payment_advice,))
                result = cursor.fetchone()
                if result[0] == 0:  # If no matching record exists
                    context.log.error(f"PaymentAdviceName '{payment_advice}' does not exist in the database.")
                    return  # Skip the update if no record is found

            synced_status = extract_synced_status(status_pa_message)
            pa_message = extract_pa_message(status_pa_message)
            update_payment_advice(context, sqlserver_conn, payment_advice, ad_synced_date, synced_status, pa_message)
            context.log.info(f"Updating ad_SyncedDate, Sync_status, and pa_message record: PaymentAdviceName={payment_advice}, ad_SyncedDate={ad_synced_date}, Sync_status={synced_status}, PA_message={pa_message}")

        elif file_type == "FI15":
            cash_issuance = format_cash_issuance_name(context, payment_advice_name)

            # Check if CashIssuanceName exists
            check_query = "SELECT COUNT(1) FROM dbo.CashIssuance WHERE CashIssuanceName = ?"
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(check_query, (cash_issuance,))
                result = cursor.fetchone()
                if result[0] == 0:  # If no matching record exists
                    context.log.error(f"CashIssuanceName '{cash_issuance}' does not exist in the database.")
                    return  # Skip the update if no record is found

            synced_status = extract_synced_status(status_pa_message)
            sap_indicator = 1
            sap_code = extract_sap_code(status_pa_message)
            sap_remarks = extract_sap_remarks(status_pa_message)
            update_cash_issuance(context, sqlserver_conn, cash_issuance, ad_synced_date, synced_status, sap_indicator, sap_code, sap_remarks)
            context.log.info(f"Updating ad_SyncedDate, Synced_status, vwlzs_SAPIndicator, vwlzs_sapcode and vwlzs_sapremarks record: CashIssuanceName={cash_issuance}, ad_SyncedDate={ad_synced_date}, Sync_status={synced_status}, vwlzs_SAPIndicator={sap_indicator}, vwlzs_sapcode={sap_code}, vwlzs_sapremarks={sap_remarks}")

    except ValueError as e:
        context.log.error(f"Date parsing error for {payment_advice_name}. Error: {e}")
    except Exception as e:
        context.log.error(f"Unexpected error while processing data row: {e}")

def extract_synced_status(status_pa_message):
    """Extracts the status from the status_pa_message."""
    if status_pa_message:
        if status_pa_message[0] == 'S':
            return 'Synced'
        elif status_pa_message[0] == 'E':
            return 'Error'

    return None

def extract_sap_code(status_pa_message):
    """Extracts the 10 characters after the dynamic prefix in the status_pa_message."""
    if not status_pa_message:
        return None  # Return None if the message is empty

    # Find the position of the ':'
    colon_index = status_pa_message.find(":")
    if colon_index == -1:
        return None  # Return None if ':' is not found

    # Extract the part of the message after ':'
    message_after_colon = status_pa_message[colon_index + 1:].strip()

    # Split the message to find the dynamic prefix (first word)
    parts = message_after_colon.split()
    if len(parts) < 2:
        return None  # Not enough data after ':'

    dynamic_prefix = parts[0]  # The first word after ':'
    value_start_index = message_after_colon.find(dynamic_prefix) + len(dynamic_prefix)

    # Extract the 10 characters after the dynamic prefix
    sap_code = message_after_colon[value_start_index:value_start_index + 10].strip()

    return sap_code

def extract_pa_message(status_pa_message):
    if status_pa_message:
        return status_pa_message
    return None

def extract_sap_remarks(status_pa_message):
    """Extracts all data after the first two characters in the status_pa_message."""
    if not status_pa_message:
        return None  # Return None if the message is empty

    # Extract everything after the first two characters
    sap_remarks = status_pa_message[2:].strip()

    return sap_remarks

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

def format_cash_issuance_name(context, cash_issuance_name):
    """Formats the cash_issuance_name string to the desired format."""
    if not cash_issuance_name:
        context.log.error("Empty CashIssuanceName.")
        return

    # Extract the first 4 characters for the prefix
    prefix_part = cash_issuance_name[:4]

    prefix = "CI" if prefix_part == "DICI" else prefix_part
    year = cash_issuance_name[4:8]
    number = cash_issuance_name[8:]

    return f"{prefix}-{year}-{number}"

def update_payment_advice(context, sqlserver_conn, payment_advice_name, ad_synced_date, synced_status, pa_message):
    """Update the PaymentAdvice table."""
    try:
        if not payment_advice_name:
            context.log.error("PaymentAdviceName must be provided.")
            return

        update_query = """
            UPDATE dbo.PaymentAdvice
            SET ad_SyncedDate = ?, Synced_status = ?, PA_message = ?
            WHERE PaymentAdviceName = ?
        """
        parameters = (ad_synced_date, synced_status, pa_message, payment_advice_name)
        with sqlserver_conn.cursor() as cursor:
            cursor.execute(update_query, parameters)
            sqlserver_conn.commit()
            context.log.info(f"Updated PaymentAdvice: {payment_advice_name}.")
    except Exception as e:
        context.log.error(f"Error updating PaymentAdvice: {e}")

def update_cash_issuance(context, sqlserver_conn, cash_issuance_name, ad_synced_date, synced_status, sap_indicator, sap_code, sap_remarks):
    """Update the CashIssuance table."""
    try:
        if not cash_issuance_name:
            context.log.error("CashIssuanceName must be provided.")
            return

        update_query = """
            UPDATE dbo.CashIssuance
            SET ad_SyncedDate = ?, Synced_status = ?, vwlzs_SAPIndicator = ?, vwlzs_sapcode = ?, vwlzs_sapremarks = ?
            WHERE CashIssuanceName = ?
        """
        parameters = (ad_synced_date, synced_status, sap_indicator, sap_code, sap_remarks, cash_issuance_name)
        with sqlserver_conn.cursor() as cursor:
            cursor.execute(update_query, parameters)
            sqlserver_conn.commit()
            context.log.info(f"Updated CashIssuance: {cash_issuance_name}.")
    except Exception as e:
        context.log.error(f"Error updating CashIssuance: {e}")

