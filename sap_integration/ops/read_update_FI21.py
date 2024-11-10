from dagster import op
from datetime import datetime
import os

@op(required_resource_keys={"sqlserver_db", "postgres_db"})
def read_update_FI21(context):
    # Define file path and extract filename
    local_path = "./ppa-flatefile/FI21_20241024235002.txt"
    filename = os.path.basename(local_path)

    # Check if the file has already been processed in SQL Server
    sqlserver_conn = context.resources.sqlserver_db
    if file_already_processed(sqlserver_conn, filename):
        context.log.info(f"File data for {filename} already updated in SQL Server with status 'Read'.")
        return None

    # Check if the file exists locally
    if not os.path.exists(local_path):
        context.log.error(f"File not found at path: {local_path}")
        return None

    # Read file content
    with open(local_path, "r") as file:
        data_raw = file.read()

    # Parse header and data rows
    lines = data_raw.strip().split('\n')
    header = lines[0].split('|')
    data_rows = [line.split('|') for line in lines[1:] if line.startswith('1')]

    header_info = {
        "record_type": header[0],
        "document_type": header[1],
        "timestamp": header[2]
    }

    # Insert file log into `SAP_Integration_Outbound`
    insert_file_log(sqlserver_conn, filename, data_raw)
    context.log.info(f"Inserted log entry for file: {filename}")

    # Process each data row and update records in SQL Server based on status
    for row in data_rows:
        if len(row) != 6:
            context.log.error(f"Invalid row format: {row}. Skipping.")
            continue
        process_data_row(context, sqlserver_conn, row)

    # Commit all updates
    sqlserver_conn.commit()
    context.log.info(f"Data update completed for document type: {header_info['document_type']} at {header_info['timestamp']}")

def file_already_processed(sqlserver_conn, filename):
    """Check if the file has already been processed in SQL Server."""
    with sqlserver_conn.cursor() as cursor:
        check_query = """
            SELECT 1 FROM dbo.SAP_Integration_Outbound WHERE filename = ? AND status = 'Read'
        """
        cursor.execute(check_query, (filename,))
        return cursor.fetchone() is not None

def insert_file_log(sqlserver_conn, filename, data_raw):
    """Insert a log entry for the file in the sap_integration_log table."""
    with sqlserver_conn.cursor() as cursor:
        insert_query = """
            INSERT INTO dbo.SAP_Integration_Outbound (file_type, filename, data_raw, status, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI21', filename, data_raw, 'Read'))
        sqlserver_conn.commit()

def process_data_row(context, sqlserver_conn, row):
    """Process a single row of data based on the status and perform updates accordingly."""
    record_indicator, company_code, payment_advice_name, status, pa_message, pad_id = row
    context.log.info(f"Processing Payment Advice: {payment_advice_name} with status: {status}")

    try:
        if status == "PRINTED" and pa_message == "SUCCESS" and "CLD" in pad_id and "EFD" not in pad_id:
            ad_bankclearance_date = extract_date_from_pad_id(pad_id, "CLD")
            context.log.info(f"Updating PRINTED, SUCCESS, and CLD record: PaymentAdviceName={payment_advice_name}, ad_bankclearance={ad_bankclearance_date}")
            update_payment_advice(sqlserver_conn, payment_advice_name, ad_bankclearance_date, 'ad_BankClearance')

        elif status == "PRINTED" and pa_message == "SUCCESS" and "CLD" in pad_id and "EFD" in pad_id:
            ad_bankclearance_date = extract_date_from_pad_id(pad_id, "CLD")
            ad_effectivedate = extract_date_from_pad_id(pad_id, "EFD")
            context.log.info(f"Updating PRINTED, SUCCESS, CLD, and EFD record: PaymentAdviceName={payment_advice_name}, ad_bankclearance={ad_bankclearance_date}, ad_effectivedate={ad_effectivedate}")
            update_payment_advice(sqlserver_conn, payment_advice_name, ad_bankclearance_date, 'ad_BankClearance', ad_effectivedate)

        elif status == "PAID" and "COD" in pad_id:
            ad_collected_date = extract_date_from_pad_id(pad_id, "COD")
            context.log.info(f"Updating PAID and COD record: PaymentAdviceName={payment_advice_name}, ad_collecteddate={ad_collected_date}")
            update_payment_advice(sqlserver_conn, payment_advice_name, ad_collected_date, 'ad_collecteddate')

    except ValueError as e:
        context.log.error(f"Date parsing error for PAD ID {pad_id} in row: {row}. Error: {e}")

def extract_date_from_pad_id(pad_id, prefix):
    """Extracts the date from the pad_id based on a specific prefix."""
    for part in pad_id.split(';'):
        if part.startswith(f"{prefix}-"):
            date_str = part.split('-')[-1]
            return datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")
    return None

def update_payment_advice(sqlserver_conn, payment_advice_name, date_value, date_field, ad_effectivedate=None):
    """Update the PaymentAdvice table based on the provided details."""
    if ad_effectivedate:
        query = """
            UPDATE dbo.PaymentAdvice
            SET status = ?, ad_BankClearance = ?, ad_effectivedate = ?
            WHERE PaymentAdviceName = ?
        """
        parameters = ('PRINTED', date_value, ad_effectivedate, payment_advice_name)
    else:
        query = f"""
            UPDATE dbo.PaymentAdvice
            SET {date_field} = ?
            WHERE PaymentAdviceName = ?
        """
        parameters = (date_value, payment_advice_name)

    with sqlserver_conn.cursor() as cursor:
        cursor.execute(query, parameters)
