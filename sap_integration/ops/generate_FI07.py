from dagster import op
from datetime import datetime

@op(required_resource_keys={"sqlserver_db"})
def generate_FI07(context):
    conn = context.resources.sqlserver_db
    cursor = conn.cursor()

    query = "SELECT TOP 10 * FROM dbo.asnaf"  # You can modify this query as needed
    cursor.execute(query)
    data = cursor.fetchall()

    if not data:
        context.log.info("No data found in the SQL Server table. Skipping file generation.")
        return None

    formatted_lines = []

    # Add the header with dynamic datetime
    date_created = datetime.now().strftime("%Y%m%d%H%M%S00")
    header = f"0|FI07|{date_created}|PPA||{len(data)}"
    formatted_lines.append(header)

    for row in data:
        # Handle potential NULL values
        name = row.Name if row.Name else "N/A"
        gender = row.Gender if row.Gender else "Unknown"
        ic_num = row.IdentificationNumIC if row.IdentificationNumIC else "N/A"
        asnaf_id = row.AsnafID if row.AsnafID else "N/A"
        street1 = row.Street1 if row.Street1 else ""
        street2 = row.Street2 if row.Street2 else ""
        street3 = row.Street3 if row.Street3 else ""
        city = row.City if row.City else ""
        postcode = row.Postcode if row.Postcode else ""
        state = row.State if row.State else ""
        country = row.Country if row.Country else ""
        phone_home = row.TelephoneNoHome if row.TelephoneNoHome else ""
        mobile_phone = row.MobilePhoneNum if row.MobilePhoneNum else ""
        email = row.Emel if row.Emel else ""
        bank_account = row.BankAccountNum if row.BankAccountNum else ""

        line1 = f"1|ZB90||2|{name}|{gender}||{ic_num}|{asnaf_id}"
        line2 = f"2|{street1}|{street2}|{street3}|||{city}|{postcode}|{state}|{country}|EN|{phone_home}||{mobile_phone}|{email}"
        line3 = f"3|0001|MY||{bank_account}|"
        line4 = f"4|MY4|{ic_num}"
        line5 = "5|001|5000002|V090||X|CDEFJTV|||X"

        formatted_lines.extend([line1, line2, line3, line4, line5])

    context.log.info(f"Extracted and formatted {len(data)} rows of data.")
    return formatted_lines
