import os
from dagster import op
from datetime import datetime


@op(required_resource_keys={"sqlserver_db", "sftp"})
def generate_FI10(context):

    # Define the SFTP path
    REMOTE_FOLDER = "FI10/Outgoing"

    # Get the SFTP connection from the resource
    sftp_conn = context.resources.sftp

    # Check if the directory exists on the SFTP server
    if not sftp_conn.exists(REMOTE_FOLDER):
        context.log.error(f"SFTP directory not found: {REMOTE_FOLDER}")
        return
    
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    # Get the current date in the required format
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Query for data1 (Direct-Asnaf)
    # query1 = """
    # SELECT * 
    # FROM dbo.GabungPA_SAP 
    # WHERE PAType IN ('Direct-Asnaf', 'Direct-Master') 
    # AND SAP_Touchpoint = 'FI10'
    # AND CONVERT(DATE, DateCreated) = ?;
    # """
    # cursor.execute(query1, (current_date,))
    # data1 = cursor.fetchall()

    # # Query for data2 (Direct-Recipient)
    # query2 = """
    # SELECT * 
    # FROM dbo.GabungPA_SAP 
    # WHERE PAType IN ('Direct-Recipient') 
    # AND SAP_Touchpoint = 'FI10'
    # AND CONVERT(DATE, DateCreated) = ?;
    # """
    # cursor.execute(query2, (current_date,))
    # data2 = cursor.fetchall()

    # Define the list of PaymentAdviceName values
    payment_advice_names = [
        'PA-2024-00000005', 'PA-2024-00000008', 'PA-2024-00000011',
        'PA-2024-00000012', 'PA-2024-00000014', 'PA-2024-00000019',
        'PA-2024-00000021', 'PA-2024-00000023', 'PA-2024-00000026',
        'PA-2024-00000028', 'PA-2024-00000032', 'PA-2024-00000033',
        'PA-2024-00000034', 'PA-2024-00000038', 'PA-2024-00000041',
        'PA-2024-00000042', 'PA-2024-00000045', 'PA-2024-00000046',
        'PA-2024-00000047', 'PA-2024-00000049', 'PA-2024-00000051',
        'PA-2024-00000052', 'PA-2024-00000054', 'PA-2024-00000059',
        'PA-2024-00000060', 'PA-2024-00000061', 'PA-2024-00000064',
        'PA-2024-00000067', 'PA-2024-00000068', 'PA-2024-00000074',
        'PA-2024-00000075', 'PA-2024-00000076', 'PA-2024-00000078',
        'PA-2024-00000080', 'PA-2024-00000084', 'PA-2024-00000085',
        'PA-2024-00000088', 'PA-2024-00000089', 'PA-2024-00000094',
        'PA-2024-00000095', 'PA-2024-00000098', 'PA-2024-00000101',
        'PA-2024-00000104', 'PA-2024-00000110', 'PA-2024-00000111',
        'PA-2024-00000112', 'PA-2024-00000125', 'PA-2024-00000126',
        'PA-2024-00000140', 'PA-2024-00000141', 'PA-2024-00000142',
        'PA-2024-00000143', 'PA-2024-00000144', 'PA-2024-00000145',
        'PA-2024-00000147', 'PA-2024-00000149', 'PA-2024-00000154',
        'PA-2024-00000156', 'PA-2024-00000157', 'PA-2024-00000158',
        'PA-2024-00000159', 'PA-2024-00000162', 'PA-2024-00000163',
        'PA-2024-00000164', 'PA-2024-00000165', 'PA-2024-00000166',
        'PA-2024-00000167', 'PA-2024-00000168', 'PA-2024-00000170',
        'PA-2024-00000171', 'PA-2024-00000173', 'PA-2024-00000174',
        'PA-2024-00000175', 'PA-2024-00000179', 'PA-2024-00000200',
        'PA-2024-00000208', 'PA-2024-00000210', 'PA-2024-00000211',
        'PA-2024-00000214', 'PA-2024-00000215', 'PA-2024-00000216',
        'PA-2024-00000217', 'PA-2024-00000218', 'PA-2024-00000219',
        'PA-2024-00000220', 'PA-2024-00000221', 'PA-2024-00000222'
    ]

    # Convert the list to a formatted string for the SQL IN clause
    formatted_payment_advice_names = ", ".join(f"'{name}'" for name in payment_advice_names)

    # Query for data1 (Direct-Asnaf and Direct-Master)
    query1 = f"""
    SELECT * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Direct-Asnaf', 'Direct-Master') 
    AND SAP_Touchpoint = 'FI10'
    AND PaymentAdviceName IN ({formatted_payment_advice_names});
    """
    cursor.execute(query1)
    data1 = cursor.fetchall()

    # Query for data2 (Direct-Recipient)
    query2 = f"""
    SELECT * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Direct-Recipient') 
    AND SAP_Touchpoint = 'FI10'
    AND PaymentAdviceName IN ({formatted_payment_advice_names});
    """
    cursor.execute(query2)
    data2 = cursor.fetchall()

    # Initialize formatted_lines with only the header initially
    formatted_lines = []

    # Add the header with dynamic datetime (no data length yet, since we will check later)
    date_created = datetime.now().strftime("%Y%m%d%H%M%S00")
    header = f"0|FI10|{date_created}|PPA||"  # We will append total_data_length if there is data
    formatted_lines.append(header)

    def write_to_flatfile_file(context, formatted_lines, filename):
        # Convert the formatted lines into a single string with newline separation
        file_content = "\n".join(formatted_lines)

        # Define the local path for temporary storage before uploading
        local_file_path = f"/tmp/{filename}"

        # Write the file content to a local file
        try:
            with open(local_file_path, "w") as file:
                file.write(file_content)
            context.log.info(f"File written to local path: {local_file_path}")
        except Exception as e:
            context.log.error(f"Error writing file to local path: {e}")
            return False

        # Upload the file to the SFTP server
        try:
            remote_file_path = f"{REMOTE_FOLDER}/{filename}"
            sftp_conn = context.resources.sftp
            with sftp_conn.open(remote_file_path, "w") as remote_file:
                remote_file.write(file_content)
            context.log.info(f"File successfully uploaded to SFTP server: {remote_file_path}")
            return True
        except Exception as e:
            context.log.error(f"Error uploading file to SFTP server: {e}")
            return False
        finally:
            # Clean up the local file
            try:
                os.remove(local_file_path)
                context.log.info(f"Temporary local file removed: {local_file_path}")
            except Exception as cleanup_error:
                context.log.error(f"Error removing temporary local file: {cleanup_error}")

    # Helper function to format DateCreated
    def format_date(date_value):
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        elif date_value:
            return datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        return ""

    # Helper function to replace None or empty values with blank
    def clean_value(value):
        return "" if value is None else str(value)

    # Helper function to check if PaymentAdviceName exists in dbo.SAP_Integration_Inbound.data_key
    def payment_advice_exists(payment_advice_name):
        check_query = """
        SELECT 1 
        FROM dbo.SAP_Integration_Inbound 
        WHERE file_type = 'FI10' AND data_key = ? AND status = 'Push to SFTP'
        """
        cursor.execute(check_query, (payment_advice_name,))
        return cursor.fetchone() is not None

    # Helper function to insert a log entry into SAP_Integration_Inbound
    def insert_inbound_log(payment_advice_name, data_raw):
        insert_query = """
        INSERT INTO dbo.SAP_Integration_Inbound (file_type, data_key, data_raw, status, created_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI10', payment_advice_name, data_raw, 'Push to SFTP'))
        conn.commit()
        context.log.info(f"Inserted log for PaymentAdviceName: {payment_advice_name}")

    # Process data1 specific to Direct-Asnaf
    def process_data1(data):
        lines_added = 0
        for row in data:
            if payment_advice_exists(clean_value(row.PaymentAdviceName)):
                context.log.info(f"Skipping {clean_value(row.PaymentAdviceName)} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{clean_value(row.PaymentAdviceName)}|||{clean_value(row.DistributionitemsName)}||1|{clean_value(row.ad_Paamount)}|-{clean_value(row.ad_Paamount)}"
            line2 = f"2|001|S|{clean_value(row.vwlzs_glaccount)}|||MYR|{clean_value(row.ad_Paamount)}||MYR|{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|||||||||||||||"
            line3 = f"2|002|K|{clean_value(row.SAPCode)}|||MYR|-{clean_value(row.ad_Paamount)}||MYR|-{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}||||||||MY||||MY|||"
            formatted_lines.extend([line1, line2, line3])
            lines_added += 3

            # Insert a log entry into SAP_Integration_Inbound
            insert_inbound_log(clean_value(row.PaymentAdviceName), "\n".join([line1, line2, line3]))

        return lines_added

    # Process data2 specific to Direct-Recipient
    def process_data2(data):
        lines_added = 0
        for row in data:
            if payment_advice_exists(clean_value(row.PaymentAdviceName)):
                context.log.info(f"Skipping {clean_value(row.PaymentAdviceName)} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{clean_value(row.PaymentAdviceName)}|||{clean_value(row.DistributionitemsName)}||1|{clean_value(row.ad_Paamount)}|-{clean_value(row.ad_Paamount)}"
            line2 = f"2|001|S|{clean_value(row.vwlzs_glaccount)}|||MYR|{clean_value(row.ad_Paamount)}||MYR|{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|||||||||||||||"
            line3 = f"2|002|K|5000002|||MYR|-{clean_value(row.ad_Paamount)}||MYR|-{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|{clean_value(row.ad_Penerimaname)}|||{clean_value(row.Street1)}|{clean_value(row.City)}|{clean_value(row.Postcode)}|{clean_value(row.Negeri)}|MY|{clean_value(row.Email)}|{clean_value(row.vwlzs_SwiftCode)}|{clean_value(row.BankAccountNo)}|MY||{clean_value(row.IdentificationNumIC)}|"
            formatted_lines.extend([line1, line2, line3])
            lines_added += 3

            # Insert a log entry into SAP_Integration_Inbound
            insert_inbound_log(clean_value(row.PaymentAdviceName), "\n".join([line1, line2, line3]))

        return lines_added

    # Process both data1 and data2 and sum up the lines added
    lines_added_data1 = process_data1(data1)
    lines_added_data2 = process_data2(data2)
    total_lines_added = lines_added_data1 + lines_added_data2

    # If no lines were added, clear the formatted_lines and log the outcome
    if total_lines_added == 0:
        context.log.info("No new data to process. Skipping file generation.")
        return False

    # Update the header with the correct total data length if lines were added
    formatted_lines[0] = f"0|FI10|{date_created}|PPA||{total_lines_added // 3}"
    context.log.info(f"Extracted and formatted {total_lines_added // 3} rows of data.")

    execution_time = datetime.now()
    filename = f"FI10_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")

    # Write the file and upload to SFTP
    success = write_to_flatfile_file(context, formatted_lines, filename)

    if success:
        context.log.info(f"File {filename} successfully processed and uploaded to SFTP server.")
    else:
        context.log.error(f"Failed to process and upload file {filename}.")
