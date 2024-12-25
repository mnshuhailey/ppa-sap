import os
from dagster import op
from datetime import datetime

@op(required_resource_keys={"sqlserver_db", "sftp"})
def generate_FI15(context):

    # Define the SFTP path
    REMOTE_FOLDER = "FI15/Outgoing"

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

    # Main data query
    main_query = "SELECT * FROM dbo.CashIssuance"
    try:
        cursor.execute(main_query)
        data = cursor.fetchall()
    except Exception as e:
        context.log.error(f"Failed to fetch data from dbo.CashIssuance: {e}")
        return None

    if len(data) == 0:
        context.log.info("No data found in the SQL Server table. Skipping file generation.")
        return None

    formatted_lines = []

    # Placeholder for the header
    date_created = datetime.now().strftime("%Y%m%d%H%M%S00")
    header = f"0|FI15|{date_created}|PPA||0"  # Initial header with total data length as 0
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

        finally:
            # Clean up the local file
            try:
                # Uncomment to enable file removal
                # os.remove(local_file_path)
                context.log.info(f"Temporary local file removed: {local_file_path}")
            except Exception as cleanup_error:
                context.log.error(f"Error removing temporary local file: {cleanup_error}")

    # Helper function to format DateCreated
    def format_date(date_value):
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        try:
            return datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        except ValueError:
            context.log.warning(f"Invalid date format for {date_value}. Skipping this record.")
            return None

    # Helper function to fetch related data with error logging
    def fetch_cash_issuance_details(query, parameter):
        try:
            cursor.execute(query, (parameter,))
            return cursor.fetchone()
        except Exception as e:
            context.log.error(f"Error fetching details for CashIssuanceName {parameter}: {e}")
            return None

    # Helper function to check if CashIssuanceName exists in dbo.SAP_Integration_Inbound.data_key
    def cash_issuance_exists(cash_issuance_name):
        check_query = """
        SELECT 1 
        FROM dbo.SAP_Integration_Inbound 
        WHERE file_type = 'FI15' AND data_key = ? AND status = 'Push to SFTP'
        """
        cursor.execute(check_query, (cash_issuance_name,))
        return cursor.fetchone() is not None

    # Helper function to insert a log entry into SAP_Integration_Inbound
    def insert_inbound_log(cash_issuance_name, data_raw):
        insert_query = """
        INSERT INTO dbo.SAP_Integration_Inbound (file_type, data_key, data_raw, status, created_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI15', cash_issuance_name, data_raw, 'Push to SFTP'))
        conn.commit()
        context.log.info(f"Inserted log for CashIssuanceName: {cash_issuance_name}")

    generated_data_count = 0  # Counter for the actual rows that get written to the file

    # Process data
    for row in data:
        # Check if this CashIssuanceName has already been processed
        if cash_issuance_exists(row.CashIssuanceName):
            context.log.info(f"Skipping {row.CashIssuanceName} as it already exists in SAP_Integration_Inbound.")
            continue

        # SQL queries for additional data
        query_cash_issuance_name = """
            SELECT B.vwlzs_glaccount, B.vwlzs_CostCenter, B.SAP_AsnafCategory, B.FundCode, B.businessArea 
            FROM dbo.CashIssuance AS A
            INNER JOIN dbo.DistributionItem AS C ON A.DistributionitemsID = C.DistributionitemsID
            INNER JOIN dbo.vwlzs_ChartOfAccount AS B ON C.ad_COAName COLLATE SQL_Latin1_General_CP1_CI_AS = B.vwlzs_name
            WHERE A.CashIssuanceName = ?
        """
        cash_issuance_name = fetch_cash_issuance_details(query_cash_issuance_name, row.CashIssuanceName)

        query_cash_issuance_name2 = """
            SELECT B.vwlzs_glaccount, B.vwlzs_CostCenter 
            FROM dbo.CashIssuance AS A
            INNER JOIN dbo.vwlzs_ChartOfAccount AS B ON A.TabungCOA COLLATE SQL_Latin1_General_CP1_CI_AS = B.vwlzs_name
            WHERE A.CashIssuanceName = ?
        """
        cash_issuance_name2 = fetch_cash_issuance_details(query_cash_issuance_name2, row.CashIssuanceName)

        # Ensure required data is available
        if not cash_issuance_name:
            context.log.warning(f"Missing details for CashIssuanceName {row.CashIssuanceName} in first query. Skipping row.")
            continue
        if not cash_issuance_name2:
            context.log.warning(f"Missing details for CashIssuanceName {row.CashIssuanceName} in second query. Skipping row.")
            continue

        # Format line content
        formatted_date = format_date(row.DateCreated)

        try:
            line1 = f"1|AGIH|{formatted_date}|{formatted_date}|ZG|MYR|||{row.DistributionitemsName}|{row.DistributionitemsName}||||2|{row.CIAmount}|-{row.CIAmount}"
            line2 = f"2|001|S|{cash_issuance_name.vwlzs_glaccount}|||MYR|{row.CIAmount}||MYR|{row.CIAmount}|||{cash_issuance_name.vwlzs_CostCenter}|{cash_issuance_name.vwlzs_CostCenter}||||{cash_issuance_name.SAP_AsnafCategory}||||D||{row.CashIssuanceName.strip()}||||||{row.vwlzs_sapcode}|{row.DistributionitemsName}|{cash_issuance_name.FundCode}|{cash_issuance_name.businessArea}|||||||||||||||"
            line3 = f"2|002|K|{cash_issuance_name2.vwlzs_glaccount}|||MYR|-{row.CIAmount}||MYR|-{row.CIAmount}|||{cash_issuance_name2.vwlzs_CostCenter}|{cash_issuance_name2.vwlzs_CostCenter}||||||||D||{row.CashIssuanceName.strip()}||||||||{cash_issuance_name.FundCode}|{cash_issuance_name.businessArea}|||||||||||||||"

            formatted_lines.extend([line1, line2, line3])
            generated_data_count += 1

            # Insert log for each generated data entry
            insert_inbound_log(row.CashIssuanceName, "\n".join([line1, line2, line3]))
        except Exception as e:
            context.log.error(f"Failed to format lines for CashIssuanceName {row.CashIssuanceName}: {e}")
            continue

    # If no lines were added, clear the formatted_lines and log the outcome
    if generated_data_count == 0:
        context.log.info("No new data to process. Skipping file generation.")
        return False

    # Update header with the actual count of generated data entries
    total_data_length = generated_data_count
    formatted_lines[0] = f"0|FI15|{date_created}|PPA||{total_data_length}"  # Update the header with the correct count

    context.log.info(f"Generated and formatted {generated_data_count} entries of data.")

    execution_time = datetime.now()
    filename = f"FI15_{execution_time.strftime('%Y%m%d%H%M%S00')}.txt"
    context.log.info(f"Generated filename: {filename}")

    # Write the file and upload to SFTP
    success = write_to_flatfile_file(context, formatted_lines, filename)

    if success:
        context.log.info(f"File {filename} successfully processed and uploaded to SFTP server.")
    else:
        context.log.error(f"Failed to process and upload file {filename}.")
