from dagster import op
from datetime import datetime


@op(required_resource_keys={"sqlserver_db"})
def generate_FI09(context):
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    # Query for data1 (Invoice-Asnaf and Invoice-Master)
    query1 = """
    SELECT * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Invoice-Asnaf', 'Invoice-Master') 
    AND SAP_Touchpoint = 'FI09'
    """
    cursor.execute(query1)
    data1 = cursor.fetchall()

    # Query for data2 (Invoice-Recipient)
    query2 = """
    SELECT * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Invoice-Recipient') 
    AND SAP_Touchpoint = 'FI09'
    """
    cursor.execute(query2)
    data2 = cursor.fetchall()

    # Initialize formatted_lines with only the header initially
    formatted_lines = []

    # Add the header with dynamic datetime (no data length yet, since we will check later)
    date_created = datetime.now().strftime("%Y%m%d%H%M%S00")
    header = f"0|FI09|{date_created}|PPA||"  # Placeholder for total_data_length
    formatted_lines.append(header)

    # Helper function to format DateCreated
    def format_date(date_value):
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        else:
            return datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")

    # Helper function to check if PaymentAdviceName exists in dbo.SAP_Integration_Inbound.data_key
    def payment_advice_exists(payment_advice_name):
        check_query = """
        SELECT 1 
        FROM dbo.SAP_Integration_Inbound 
        WHERE file_type = 'FI09' AND data_key = ? AND status = 'Push to SFTP'
        """
        cursor.execute(check_query, (payment_advice_name,))
        return cursor.fetchone() is not None

    # Helper function to insert a log entry into SAP_Integration_Inbound
    def insert_inbound_log(payment_advice_name, data_raw):
        insert_query = """
        INSERT INTO dbo.SAP_Integration_Inbound (file_type, data_key, data_raw, status, created_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        cursor.execute(insert_query, ('FI09', payment_advice_name, data_raw, 'Push to SFTP'))
        conn.commit()
        context.log.info(f"Inserted log for PaymentAdviceName: {payment_advice_name}")

    # Process data1 specific to Invoice-Asnaf and Invoice-Master
    def process_data1(data):
        lines_added = 0
        for row in data:
            if payment_advice_exists(row.PaymentAdviceName):
                context.log.info(f"Skipping {row.PaymentAdviceName} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{row.PaymentAdviceName}|||{row.DistributionitemsName}||1|{row.ad_Paamount}|-{row.ad_Paamount}"
            line2 = f"2|001|S|{row.vwlzs_glaccount}|||MYR|{row.ad_Paamount}||MYR|{row.ad_Paamount}|||{row.vwlzs_CostCenter}|{row.vwlzs_CostCenter}||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|||||||||||||||"
            line3 = f"2|002|K|{row.SAPCode}|||MYR|-{row.ad_Paamount}||MYR|-{row.ad_Paamount}||||||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}||||||||MY||||MY|||"
            formatted_lines.extend([line1, line2, line3])
            lines_added += 3

            # Insert a log entry into SAP_Integration_Inbound
            insert_inbound_log(row.PaymentAdviceName, "\n".join([line1, line2, line3]))

        return lines_added

    # Process data2 specific to Invoice-Recipient
    def process_data2(data):
        lines_added = 0
        for row in data:
            if payment_advice_exists(row.PaymentAdviceName):
                context.log.info(f"Skipping {row.PaymentAdviceName} as it already exists in SAP_Integration_Inbound.")
                continue

            date_created = format_date(row.DateCreated)
            line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{row.PaymentAdviceName}|||{row.DistributionitemsName}||1|{row.ad_Paamount}|-{row.ad_Paamount}"
            line2 = f"2|001|S|{row.vwlzs_glaccount}|||MYR|{row.ad_Paamount}||MYR|{row.ad_Paamount}|||{row.vwlzs_CostCenter}|{row.vwlzs_CostCenter}||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|||||||||||||||"
            line3 = f"2|002|K|{row.SAPCode}|||MYR|-{row.ad_Paamount}||MYR|-{row.ad_Paamount}||||||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|{row.ad_Penerimaname}|||{row.Street1}|{row.City}|{row.Postcode}|{row.Negeri}|MY|{row.Email}|{row.vwlzs_SwiftCode}|{row.BankAccountNo}|MY||{row.IdentificationNumIC}|"
            formatted_lines.extend([line1, line2, line3])
            lines_added += 3

            # Insert a log entry into SAP_Integration_Inbound
            insert_inbound_log(row.PaymentAdviceName, "\n".join([line1, line2, line3]))

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
    formatted_lines[0] = f"0|FI09|{date_created}|PPA||{total_lines_added // 3}"
    context.log.info(f"Extracted and formatted {total_lines_added // 3} rows of data.")

    return formatted_lines
