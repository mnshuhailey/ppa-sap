from dagster import op
from datetime import datetime
import pysftp
import os

# @op(required_resource_keys={"sqlserver_db", "sftp"})
@op(required_resource_keys={"sqlserver_db"})
def read_update_FI21(context):
    # Establish SFTP connection (commented for temporary local testing)
    # sftp = context.resources.sftp
    # with sftp.open('/path/to/yourfile.txt') as file:
    #     data = file.read().decode('utf-8')

    # For temporary, read the file's content from the local path
    local_path = "./ppa-flatefile/FI21_20241024235002.txt"
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

        # Update if the status is "PRINTED", pa_message is "SUCCESS", and pad_id does not start with "EFD"
        if status == "PRINTED" and pa_message == "SUCCESS" and "CLD" in pad_id and "EFD" not in pad_id:
            date_str = pad_id.split('-')[-1]
            ad_bankclearance_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")

            # Log details before updating
            context.log.info(f"Updating PRINTED, SUCCESS, and CLD record: PaymentAdviceName={payment_advice_name}, ad_bankclearance={ad_bankclearance_date}")

            query = """
            UPDATE dbo.PaymentAdvice
            SET ad_BankClearance = ?
            WHERE PaymentAdviceName = ?
            """
            cursor.execute(query, (ad_bankclearance_date, payment_advice_name))

        # Update if the status is "PRINTED", pa_message is "SUCCESS", and pad_id contains both "CLD" and "EFD"
        elif status == "PRINTED" and pa_message == "SUCCESS" and "CLD" in pad_id and "EFD" in pad_id:
            # Extract dates for ad_bankclearance and ad_effectivedate from pad_id
            ad_bankclearance_date, ad_effectivedate = None, None
            for part in pad_id.split(';'):
                if part.startswith("CLD-"):
                    ad_bankclearance_date = datetime.strptime(part.split('-')[-1], "%y%m%d").strftime("%Y%m%d")
                elif part.startswith("EFD-"):
                    ad_effectivedate = datetime.strptime(part.split('-')[-1], "%y%m%d").strftime("%Y%m%d")

            # Log details before updating
            context.log.info(f"Updating PRINTED, SUCCESS, CLD and EFD record: PaymentAdviceName={payment_advice_name}, ad_bankclearance={ad_bankclearance_date}, ad_effectivedate={ad_effectivedate}")

            query = """
            UPDATE dbo.PaymentAdvice
            SET status = ?, ad_BankClearance = ?, ad_effectivedate = ?
            WHERE PaymentAdviceName = ?
            """
            cursor.execute(query, (status, ad_bankclearance_date, ad_effectivedate, payment_advice_name))

        # Update if the status is "PAID" and COD
        elif status == "PAID" and "COD" in pad_id:
            date_str = pad_id.split('-')[-1]
            ad_collected_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y%m%d")

            # Log details before updating
            context.log.info(f"Updating PAID and COD record: PaymentAdviceName={payment_advice_name}, status={status}, ad_collecteddate={ad_collected_date}")

            query = """
            UPDATE dbo.PaymentAdvice
            SET ad_collecteddate = ?
            WHERE PaymentAdviceName = ?
            """
            cursor.execute(query, (ad_collected_date, payment_advice_name))

    # Commit transaction
    conn.commit()

    # Log completion message
    context.log.info(
        f"Data update completed for document type: {header_info['document_type']} at {header_info['timestamp']}")
