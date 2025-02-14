import os
from math import ceil
from dagster import op
from datetime import datetime

@op(required_resource_keys={"sqlserver_db", "sftp"})
# @op(required_resource_keys={"sqlserver_db"})
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

    # # Query for data1 (Direct-Asnaf)
    # query1 = """
    # SELECT * FROM dbo.GabungPA_SAP 
    # WHERE PAType IN ('Direct-Asnaf', 'Direct-Master')
    # AND SAP_Touchpoint = 'FI10'
    # AND PaymentAdviceName IN (
    #     SELECT PaymentAdviceName FROM [PPA_CORE].[dbo].[PAxsyncv212022025]
    # );
    # """
    # cursor.execute(query1)
    # data1 = cursor.fetchall()

    # # Query for data2 (Direct-Recipient)
    # query2 = """
    # SELECT * FROM dbo.GabungPA_SAP 
    # WHERE PAType IN ('Direct-Recipient') 
    # AND SAP_Touchpoint = 'FI10'
    # AND PaymentAdviceName IN (
    #     SELECT PaymentAdviceName FROM [PPA_CORE].[dbo].[PAxsyncv212022025]
    # );
    # """
    # cursor.execute(query2)
    # data2 = cursor.fetchall()

    # Get current date in YYYY-MM-DD format
    # current_date = datetime.now().strftime('%Y-%m-%d')

    current_date = '2025-02-12'

    # Define full-day range
    start_date = f"{current_date} 00:00:00"
    end_date = f"{current_date} 23:59:59"

    # Query for data1 (Direct-Asnaf & Direct-Master)
    query1 = """
    SELECT * FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Direct-Asnaf', 'Direct-Master')
    AND SAP_Touchpoint = 'FI10'
    AND DateCreated BETWEEN ? AND ?
    """
    cursor.execute(query1, (start_date, end_date))
    data1 = cursor.fetchall()

    # Query for data2 (Direct-Recipient)
    query2 = """
    SELECT * FROM dbo.GabungPA_SAP 
    WHERE PAType = 'Direct-Recipient' 
    AND SAP_Touchpoint = 'FI10'
    AND DateCreated BETWEEN ? AND ?
    """
    cursor.execute(query2, (start_date, end_date))
    data2 = cursor.fetchall()


    header_template = "0|FI10|{date_created}|PPA||{line_count}"

    def format_date(date_value):
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        elif date_value:
            return datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        return ""

    def clean_value(value):
        return "" if value is None else str(value)

    def insert_inbound_log(payment_advice_name, data_raw):
        insert_query = """
        INSERT INTO dbo.SAP_Integration_Inbound (filename, file_type, data_key, data_raw, status, created_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, (filename, 'FI10', payment_advice_name, data_raw, 'Push to SFTP'))
        conn.commit()
        context.log.info(f"Inserted log for PaymentAdviceName: {payment_advice_name}")

    def payment_advice_exists(payment_advice_name):
        check_query = """
        SELECT 1 
        FROM dbo.SAP_Integration_Inbound 
        WHERE file_type = 'FI10' AND data_key = ? AND status = 'Push to SFTP'
        """
        cursor.execute(check_query, (payment_advice_name,))
        return cursor.fetchone() is not None
    
    def write_to_flatfile_file(context, formatted_lines, filename):
        # Convert the formatted lines into a single string with newline separation
        file_content = "\n".join(formatted_lines)

        # Define the local path for temporary storage before uploading
        local_file_path = f"/home/shuhailey/ppa-flatfile-generated/{filename}"

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
            # Ensure the SFTP connection is active
            if not sftp_conn:
                context.log.error("SFTP connection is not initialized.")
                return False

            # Define the remote file path
            remote_file_path = f"{REMOTE_FOLDER}/{filename}"

            # Upload the file
            context.log.info(f"Uploading file to SFTP server at path: {remote_file_path}")
            with sftp_conn.open(remote_file_path, "w") as remote_file:
                remote_file.write(file_content)
            context.log.info(f"File successfully uploaded to SFTP server: {remote_file_path}")
            return True

        except IOError as io_error:
            context.log.error(f"IO error during file upload: {io_error}")
            return False

        except Exception as e:
            context.log.error(f"Error uploading file to SFTP server: {e}")
            return False

        # finally:
        #     # Clean up the local file
        #     try:
        #         # Uncomment to enable file removal
        #         # os.remove(local_file_path)
        #         context.log.info(f"Temporary local file: {local_file_path}")
        #     except Exception as cleanup_error:
        #         context.log.error(f"Error removing temporary local file: {cleanup_error}")
    
    def write_to_flatfile_file_local(context, formatted_lines, filename):
        # Convert the formatted lines into a single string with newline separation
        file_content = "\n".join(formatted_lines)

        # Define the local folder for generated flat files
        local_folder_path = os.path.join(os.getcwd(), "ppa-flatfile-generated")

        # Ensure the directory exists
        os.makedirs(local_folder_path, exist_ok=True)

        # Define the full local file path
        local_file_path = os.path.join(local_folder_path, filename)

        # Write the file content to a local file
        try:
            with open(local_file_path, "w") as file:
                file.write(file_content)
            context.log.info(f"File written to local path: {local_file_path}")
        except Exception as e:
            context.log.error(f"Error writing file to local path: {e}")
            return False

        # Optional: Add further processing, e.g., SFTP upload, here if needed
        return True

    def process_data1(data):
        formatted_lines = []
        for row in data:
            if payment_advice_exists(clean_value(row.PaymentAdviceName)):
                context.log.info(f"Skipping {clean_value(row.PaymentAdviceName)} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{clean_value(row.PaymentAdviceName)}|||{clean_value(row.DistributionitemsName)}||1|{clean_value(row.ad_Paamount)}|-{clean_value(row.ad_Paamount)}"
            line2 = f"2|001|S|{clean_value(row.vwlzs_glaccount)}|||MYR|{clean_value(row.ad_Paamount)}||MYR|{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|||||||||||||||"
            line3 = f"2|002|K|{clean_value(row.SAPCode)}|||MYR|-{clean_value(row.ad_Paamount)}||MYR|-{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}||||||||MY||||MY|||"
            formatted_lines.extend([line1, line2, line3])

            # Log each PaymentAdviceName
            insert_inbound_log(clean_value(row.PaymentAdviceName), "\n".join([line1, line2, line3]))

        return formatted_lines

    def process_data2(data):
        formatted_lines = []
        for row in data:
            if payment_advice_exists(clean_value(row.PaymentAdviceName)):
                context.log.info(f"Skipping {clean_value(row.PaymentAdviceName)} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{clean_value(row.PaymentAdviceName)}|||{clean_value(row.DistributionitemsName)}||1|{clean_value(row.ad_Paamount)}|-{clean_value(row.ad_Paamount)}"
            line2 = f"2|001|S|{clean_value(row.vwlzs_glaccount)}|||MYR|{clean_value(row.ad_Paamount)}||MYR|{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|||||||||||||||"
            line3 = f"2|002|K|5000002|||MYR|-{clean_value(row.ad_Paamount)}||MYR|-{clean_value(row.ad_Paamount)}|||{clean_value(row.COA_CostCenter)}|{clean_value(row.COA_CostCenter)}||||{clean_value(row.SAP_AsnafCategory)}||||{clean_value(row.ad_PenerimaMOP)}||{clean_value(row.AA_invoice)}|{clean_value(row.remark)}|||||{clean_value(row.ad_sapcommittedreference)}|{clean_value(row.DistributionitemsName)}|{clean_value(row.FundCode)}|{clean_value(row.businessArea)}|{clean_value(row.ad_Penerimaname)}|||{clean_value(row.Street1)}|{clean_value(row.City)}|{clean_value(row.Postcode)}|{clean_value(row.Negeri)}|MY|{clean_value(row.Email)}|{clean_value(row.vwlzs_SwiftCode)}|{clean_value(row.BankAccountNo)}|MY||{clean_value(row.IdentificationNumIC)}|"
            formatted_lines.extend([line1, line2, line3])

            # Log each PaymentAdviceName
            insert_inbound_log(clean_value(row.PaymentAdviceName), "\n".join([line1, line2, line3]))

        return formatted_lines

    # Define the output folder
    # output_folder = os.path.join(os.getcwd(), "ppa-flatfile-generated")
    # os.makedirs(output_folder, exist_ok=True)

    chunk_size = 1000

    # Process and generate files in chunks
    for dataset, process_function in [(data1, process_data1), (data2, process_data2)]:
        if not dataset:  # Skip if dataset is empty
            context.log.info(f"Skipping {process_function.__name__}: No data found.")
            continue

        lines_processed = 0
        while lines_processed < len(dataset):
            chunk = dataset[lines_processed:lines_processed + chunk_size]
            filename = f"FI10_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.txt"

            processed_lines = process_function(chunk)

            if not processed_lines:  # Skip writing if chunk processing returned no data
                context.log.info(f"Skipping file creation for {filename}: No valid data in chunk.")
            else:
                # Add header and write to file
                header = header_template.format(
                    date_created=datetime.now().strftime("%Y%m%d%H%M%S"), 
                    line_count=len(processed_lines) // 3
                )
                processed_lines.insert(0, header)

                write_to_flatfile_file(context, processed_lines, filename)

                context.log.info(f"File {filename} written successfully with {len(processed_lines) // 3} entries.")

            lines_processed += chunk_size  # ✅ Ensure progress in loop

