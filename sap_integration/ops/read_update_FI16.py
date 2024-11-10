from dagster import op
from datetime import datetime
import os

@op(required_resource_keys={"sqlserver_db", "postgres_db"})
def read_update_FI16(context):
    # Define file path and extract filename
    local_path = "./ppa-flatefile/FI16_20241024180058.txt"
    filename = os.path.basename(local_path)

    # Check if the file has already been processed
    sqlserver_conn = context.resources.sqlserver_db
    with sqlserver_conn.cursor() as cursor:
        check_query = """
            SELECT 1 FROM dbo.SAP_Integration_Outbound WHERE filename = ? AND status = 'Read'
        """
        cursor.execute(check_query, (filename,))
        if cursor.fetchone():
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

    # Process each data row and update records based on status
    for row in data_rows:
        if len(row) != 6:
            context.log.error(f"Invalid row format: {row}. Skipping.")
            continue

        process_data_row(context, sqlserver_conn, header_info["timestamp"], row)

    # Commit all updates
    sqlserver_conn.commit()
    context.log.info(f"Data update completed for document type: {header_info['document_type']} at {header_info['timestamp']}")

def insert_file_log(sqlserver_conn, filename, data_raw):
    """Insert a log entry for the file in the SAP_Integration_Outbound table."""
    with sqlserver_conn.cursor() as cursor:
        insert_query = """
            INSERT INTO dbo.SAP_Integration_Outbound (file_type, filename, data_raw, status, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI16', filename, data_raw, 'Read'))

def process_data_row(context, sqlserver_conn, timestamp, row):
    """Process a single row of data based on the status and perform updates accordingly."""
    record_indicator, company_code, payment_advice_name, status, pa_message, pad_id = row
    context.log.info(f"Processing Payment Advice: {payment_advice_name} with status: {status}")

    date_str = pad_id.split('-')[-1]
    try:
        if status == "PRINTED":
            ad_printed_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")
            update_payment_advice(sqlserver_conn, payment_advice_name, timestamp, status, pa_message, ad_printed_date, 'ad_PrintedDate')
            context.log.info(f"Updated PRINTED record for Payment Advice Name: {payment_advice_name}")
        elif status == "VOIDED":
            ad_voiddate = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")
            update_payment_advice(sqlserver_conn, payment_advice_name, timestamp, status, pa_message, ad_voiddate, 'ad_voiddate')
            context.log.info(f"Updated VOIDED record for Payment Advice Name: {payment_advice_name}")
    except ValueError as e:
        context.log.error(f"Date parsing error for PAD ID {pad_id} in row: {row}. Error: {e}")

def update_payment_advice(sqlserver_conn, payment_advice_name, timestamp, status, pa_message, date_value, date_field):
    """Update the PaymentAdvice table based on the provided details."""
    query = f"""
        UPDATE dbo.PaymentAdvice
        SET Synced_status = 'Synced', ad_saprefno = ?, status = ?, PA_message = ?, {date_field} = ?
        WHERE PaymentAdviceName = ?
    """
    with sqlserver_conn.cursor() as cursor:
        cursor.execute(query, (timestamp, status, pa_message, date_value, payment_advice_name))

