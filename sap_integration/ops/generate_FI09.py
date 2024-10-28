from dagster import op
from datetime import datetime

@op(required_resource_keys={"sqlserver_db"})
def generate_FI09(context):
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    # First query
    query1 = """
    SELECT TOP 100 * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Invoice-Asnaf', 'Invoice-Master') 
    AND SAP_Touchpoint = 'FI09'
    """
    cursor.execute(query1)
    data1 = cursor.fetchall()

    # Then the second query
    query2 = """
    SELECT TOP 100 * 
    FROM dbo.GabungPA_SAP 
    WHERE PAType IN ('Invoice-Recipient') 
    AND SAP_Touchpoint = 'FI09'
    """
    cursor.execute(query2)
    data2 = cursor.fetchall()

    # Ensure both data1 and data2 have data before proceeding
    total_data_length = len(data1) + len(data2)
    if total_data_length == 0:
        context.log.info("No data found in the SQL Server table. Skipping file generation.")
        return None

    formatted_lines = []

    # Add the header with dynamic datetime and combined data length
    date_created = datetime.now().strftime("%Y%m%d%H%M%S00")
    header = f"0|FI09|{date_created}|PPA||{total_data_length}"
    formatted_lines.append(header)

    # Helper function to format DateCreated
    def format_date(date_value):
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        else:
            # Convert to datetime if not already a datetime object
            return datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")

    # Process data1
    for row in data1:
        date_created = format_date(row.DateCreated)
        line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{row.PaymentAdviceName}|||{row.DistributionitemsName}||1|{row.ad_Paamount}|-{row.ad_Paamount}"
        line2 = f"2|001|S|{row.vwlzs_glaccount}|||MYR|{row.ad_Paamount}||MYR|{row.ad_Paamount}|||{row.vwlzs_CostCenter}|{row.vwlzs_CostCenter}||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|||||||||||||||"
        line3 = f"2|002|K|{row.SAPCode}|||MYR|-{row.ad_Paamount}||MYR|-{row.ad_Paamount}||||||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}||||||||MY||||MY|||"
        formatted_lines.extend([line1, line2, line3])

    # Process data2
    for row in data2:
        date_created = format_date(row.DateCreated)
        line1 = f"1|AGIH|{date_created}|{date_created}|ZB|MYR|||{row.PaymentAdviceName}|||{row.DistributionitemsName}||1|{row.ad_Paamount}|-{row.ad_Paamount}"
        line2 = f"2|001|S|{row.vwlzs_glaccount}|||MYR|{row.ad_Paamount}||MYR|{row.ad_Paamount}|||{row.vwlzs_CostCenter}|{row.vwlzs_CostCenter}||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|||||||||||||||"
        line3 = f"2|002|K|{row.SAPCode}|||MYR|-{row.ad_Paamount}||MYR|-{row.ad_Paamount}||||||||{row.SAP_AsnafCategory}||||{row.ad_PenerimaMOP}||{row.AA_invoice}|{row.remark}|||||{row.ad_sapcommittedreference}|{row.DistributionitemsName}|{row.FundCode}|{row.businessArea}|{row.ad_Penerimaname}|||{row.Street1}|{row.City}|{row.Postcode}|{row.Negeri}|MY|{row.Email}|{row.vwlzs_SwiftCode}|{row.BankAccountNo}|MY||{row.IdentificationNumIC}|"
        formatted_lines.extend([line1, line2, line3])

    context.log.info(f"Extracted and formatted {total_data_length} rows of data.")
    return formatted_lines
