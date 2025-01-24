from datetime import datetime, timezone, timedelta
import os
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructType, StructField

# SET Widgets
dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# GET Widgets
azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")

# GET Secrets
azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set(f"fs.azure.sas.{azure_storage_container_name}.{azure_storage_account_name}.blob.core.windows.net", azure_storage_sas_token)

# Constants
project_path_prefix: str = "EcomKPI/Demand"
flow = "Normal"

# Azure Storage Paths
azure_storage_path_prefix: str = f"wasbs://{azure_storage_container_name}@{azure_storage_account_name}.blob.core.windows.net"
inbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, flow , "Inbound/ebisumart/JP")
outbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix , "Outbound/ebisumart/JP")
archive_inbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, flow , "Archive/Inbound/ebisumart/JP")
tender_mapping_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, "Mapping/EbisumartTenderMapping.csv")
job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# Read Tender Mapping
ebisumart_tender_schema: StructType = StructType([
    StructField("TenderID", StringType(), False),
    StructField("TenderType", StringType(), False)
])
ebisumart_tender_mapping_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(ebisumart_tender_schema).load(tender_mapping_path)
ebisumart_tender_mapping_df.createOrReplaceTempView("tender_mapping")

# Read Item Mapping
item_mapping_query: str = """
    SELECT 
        product.THK_EANCODE AS "Barcode", 
        product.PRODUCTMASTERNUMBER AS "Style", 
        product.PRODUCTCOLORID AS "Color", 
        product.PRODUCTSIZEID AS "Size"
    FROM [dbo].[EcoResProductVariantStaging] product WITH (NOLOCK)
    WHERE product.PRODUCTCONFIGURATIONID = 'A'
"""
secondary_db_options: Dict[str, str] = {
    "host": dbutils.secrets.get("storagescope", "d365-db-host"),
    "port": "1433",
    "database": dbutils.secrets.get("storagescope", "d365-db-name"),
    "dbtable": item_mapping_query,
    "user": dbutils.secrets.get("storagescope", "d365-db-username"),
    "password": dbutils.secrets.get("storagescope", "d365-db-password"),
    "reliabilityLevel": "NO_DUPLICATES",
    "isolationLevel": "READ_UNCOMMITTED",
    "tableLock": False,
    "numPartitions": 4
}
item_mapping_df: DataFrame = spark.read.format("sqlserver").options(**secondary_db_options).load()
item_mapping_df.createOrReplaceTempView("item_mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inbound Files

# COMMAND ----------

# Read Inbound Files
ebisumart_raw_path = os.path.join(inbound_path, "*.txt")
ebisumart_raw_df: DataFrame = spark.read.format("text").options(wholetext=False, lineSep="\n",).load(ebisumart_raw_path)
ebisumart_raw_df.createOrReplaceTempView("ebisumart_raw")

# Transformation

# Convert Inbound files to structured table
sequenced_df: DataFrame = spark.sql("""
    with exploded_df as (
        SELECT 
            recordId,
            EXPLODE(SPLIT(url_decode(value), '&')) AS arrayToRow,
        FROM ebisumart_raw
    ),
    converted_to_rows_df as (
        SELECT recordId,
            SPLIT_PART(arrayToRow, '=', 1) AS columnName,
            SPLIT_PART(arrayToRow, '=', 2) AS columnValue,
        FROM exploded_df
    )
    SELECT
        CASE WHEN REGEXP_LIKE(columnName, '(\\d+)$') THEN REGEXP_EXTRACT(columnName, '(\\d+)$', 1) END AS Sequence,
        CASE WHEN REGEXP_LIKE(columnName, '^(.*?)(?:\\d+)$') THEN REGEXP_EXTRACT(columnName, '^(.*?)(?:\\d+)$', 1) END AS Column_Name_With_Sequence
    FROM converted_to_rows_df
""")
sequenced_df.createOrReplaceTempView("sequenced_df")


columns = [
    "ADDR1", "ADDR2", "ADDR3", "ADD_POINT_SUM", "AUTO_CANCEL_MAIL_DATE", "BIKO", "CANCEL_DATE", "CANCEL_MAIL_DATE",
    "COUPON_ID", "COUPON_WARIBIKI", "CREDIT_COUNT", "CREDIT_KIND", "CREDIT_LIMIT", "CREDIT_NAME", "CREDIT_NO", "DAIBIKI",
    "DAIBIKI_TAX", "DEL_DATE", "DOWNLOAD_DATE", "FREE_ITEM1", "FREE_ITEM2", "FREE_ITEM3", "FREE_ITEM4", "FREE_ITEM5",
    "FREE_ITEM6", "FREE_ITEM7", "FREE_ITEM8", "FREE_ITEM9", "FREE_ITEM10", "FREE_ITEM11", "FREE_ITEM12", "FREE_ITEM13",
    "FREE_ITEM14", "FREE_ITEM15", "FREE_ITEM33", "FREE_ITEM34", "FREE_ITEM35", "FREE_ITEM36", "F_KANA", "F_NAME", "HAISO_ID", 
    "HARAIKOMI_URL", "ITEM_CD", "KESSAI_ID", "L_KANA", "L_NAME", "MEMBER_ID", "MEMBER_WARIBIKI_SUM", "MOBILE_MAIL", 
    "NETMILE_ADD_POINT_ALL", "ORDER_DATE", "ORDER_DISP_NO", "ORDER_D_FREE_ITEM31_1", "ORDER_H_ACCESS_BROWSER_KBN", 
    "ORDER_MAIL_DATE", "ORDER_NO", "PAYMENT_DATE", "PAYMENT_MONEY", "PAY_LIMIT", "PC_MAIL", "PC_MOBILE_KBN", "POINT_USE", 
    "POINT_WARIBIKI", "RECEIPT_NO", "REMINDER_MAIL_DATE", "SEIKYU", "SEND_ADDR1", "SEND_ADDR2", "SEND_ADDR3", "SEND_DATE", 
    "SEND_F_KANA", "SEND_F_NAME", "SEND_HOPE_DATE", "SEND_HOPE_TIME", "SEND_L_KANA", "SEND_L_NAME", "SEND_MAIL", 
    "SEND_MAIL_DATE", "SEND_TEL", "SEND_ZIP", "SESSION_ID", "SHIP_SLIP_NO", "SHIRE_SUM", "SORYO", "SORYO_TAX", 
    "SYOUKAI_NO", "TAX", "TEIKA_SUM", "TEIKA_SUM_TAX", "TEL", "USER_AGENT", "VERI_TORIHIKI_ID", "ZIP"
]
pivot_columns = ", ".join([f"'{col}'" for col in columns])
header = spark.sql(f"""
    SELECT *
          FROM 
            (SELECT recordId, columnName, columnValue from sequenced_df)
          PIVOT
            (first(columnValue) FOR columnName in ({pivot_columns}))
""")
header.createOrReplaceTempView("header")

parsed_header_df = spark.sql("""
    SELECT 
        recordId,
        CASE 
            WHEN ORDER_D_FREE_ITEM31_1 = '' THEN NULL 
            ELSE ORDER_D_FREE_ITEM31_1 
        END AS Item_Type,
        CASE 
            WHEN ORDER_D_FREE_ITEM31_1 = 'C' THEN '57037'
            WHEN ORDER_D_FREE_ITEM31_1 = 'L' THEN '56001'
            ELSE '57036'
        END AS eCom_Store_ID,
        REGEXP_REPLACE(ORDER_DISP_NO, '^(?:ORDER|e)', '') AS Transaction_Nbr,
        DATE_FORMAT(TO_TIMESTAMP(ORDER_DATE, 'yyyy/MM/dd'), 'yyyy-MM-dd HH:mm:ss') AS Order_Date,
        DATE_FORMAT(TO_TIMESTAMP(CANCEL_DATE, 'yyyy/MM/dd'), 'yyyy-MM-dd HH:mm:ss') AS Cancel_Date,
        'JP' AS Country,
        CAST(LEFT(ORDER_H_ACCESS_BROWSER_KBN, 1) AS INT) AS Device_Type_ID,
        CASE 
            WHEN CAST(LEFT(ORDER_H_ACCESS_BROWSER_KBN, 1) AS INT) BETWEEN 1 AND 4 THEN 
                ELEMENT_AT(ARRAY('DESKTOP', 'MOBILE', 'MOBILE PHONE', 'TABLET'), CAST(LEFT(ORDER_H_ACCESS_BROWSER_KBN, 1) AS INT))
            ELSE 'OTHER' 
        END AS Device_Type,
        NULL AS Employee_Purchase,
        'Website' AS Order_Channel,
        CASE 
            WHEN MEMBER_ID = '' THEN NULL 
            ELSE MEMBER_ID 
        END AS Customer_ID,
        CASE 
            WHEN SHIP_SLIP_NO = '' THEN NULL 
            ELSE SHIP_SLIP_NO 
        END AS Shipment_ID,
        NULL AS Shipment_Method,
        'JP' AS Ship_To_Country,
        NULL AS Ship_To,
        NULL AS Bill_To,
        CASE 
            WHEN COUPON_ID IS NULL OR COUPON_ID = '' THEN 'N' 
            ELSE 'Y' 
        END AS Promotion_Coupons,
        CAST(SORYO_TAX AS DOUBLE) AS Freight_Tax,
        CAST(TEIKA_SUM AS DOUBLE) + CAST(TEIKA_SUM_TAX AS DOUBLE) AS Demand_Gross,
        CAST(SEIKYU AS DOUBLE) - CAST(TAX AS DOUBLE) - CAST(DAIBIKI AS DOUBLE) - 
        CAST(SORYO AS DOUBLE) - CAST(COUPON_WARIBIKI AS DOUBLE) - CAST(FREE_ITEM11 AS DOUBLE) AS Demand_Net,
        NULL AS Employee_ID,
        'JP' AS Bill_To_Country,
        'JPY' AS Demand_Currency,
        CAST(SORYO AS DOUBLE) AS Freight_Amount,
        CAST(SHIRE_SUM AS DOUBLE) - CAST(TEIKA_SUM AS DOUBLE) + 
        CASE 
            WHEN ORDER_D_FREE_ITEM31_1 = 'C' THEN CAST(FREE_ITEM33 AS DOUBLE) 
            ELSE 0 
        END AS Discount_Amount,
        CAST(COUPON_WARIBIKI AS DOUBLE) AS Coupon_Amount,
        CAST(TAX AS DOUBLE) AS Tax,
        CAST(DAIBIKI AS DOUBLE) AS CoD_Charge,
        CAST(DAIBIKI_TAX AS DOUBLE) AS CoD_Charge_Tax,
        CAST(SORYO AS DOUBLE) AS Shipping_Fee,
        CAST(SORYO_TAX AS DOUBLE) AS Shipping_Fee_Tax,
        CAST(SEIKYU AS DOUBLE) AS Line_Total,
        KESSAI_ID AS Tender_ID,
        CAST(SEIKYU AS DOUBLE) AS Tender_Amt,
        CAST(FREE_ITEM11 AS DOUBLE) AS Gift_Charge,
        CAST(FREE_ITEM11 AS DOUBLE) * 0.1 AS Gift_Tax,
        -1 AS Jda_Order
    FROM header
""")

parsed_header_df.createOrReplaceTempView("parsed_header_df")
# Header final table
header_final = spark.sql("""
    SELECT 
        eCom_Store_ID,
        Transaction_Nbr,
        Order_Date,
        Country,
        Device_Type,
        COALESCE(Employee_Purchase, 'NULL') AS Employee_Purchase,
        Order_Channel,
        COALESCE(Customer_ID, 'NULL') AS Customer_ID,
        COALESCE(Ship_To, 'NULL') AS Ship_To,
        COALESCE(Bill_To, 'NULL') AS Bill_To,
        Promotion_Coupons,
        Freight_Tax,
        Demand_Net,
        COALESCE(Employee_ID, 'NULL') AS Employee_ID,
        Bill_To_Country,
        Freight_Amount,
        Discount_Amount,
        Coupon_Amount,
        Tax
    FROM parsed_header_df
    WHERE Cancel_Date IS NULL
""")

# Payment final table
payment_final = spark.sql("""
    WITH demand_payment AS (
        SELECT *
        FROM parsed_header_df
        WHERE Cancel_Date IS NULL
    )
    SELECT
        payment.eCom_Store_ID,
        payment.Transaction_Nbr,
        payment.Order_Date,
        tender.TenderType AS Tender,
        payment.Tender_Amt,
        payment.Demand_Currency
    FROM demand_payment AS payment
    LEFT JOIN tender_mapping AS tender ON payment.Tender_ID = tender.TenderID
    """)

# Line table
columns = [
    "ORDER_D_NO", "ITEM_NAME", "ITEM_ITEMPROPERTY_CD", "ORDER_D_FREE_ITEM1",
    "ORDER_D_FREE_ITEM2", "ORDER_D_FREE_ITEM3", "ORDER_D_FREE_ITEM4",
    "ORDER_D_FREE_ITEM5", "ORDER_D_FREE_ITEM31", "ORDER_D_FREE_ITEM32",
    "ORDER_D_FREE_ITEM35", "ORDER_D_FREE_ITEM36", "ORDER_D_FREE_ITEM37",
    "ORDER_D_FREE_ITEM38", "OUTPUT_FLG", "QUANTITY", "SHIRE_PRICE",
    "TEIKA", "MEMBER_WARIBIKI", "ADD_POINT"
]
line_final = spark.sql(f"""
    WITH line as (
        SELECT recordId,
            Sequence,
            {
                    ",".join(
                        [f"FIRST(CASE WHEN Column_Name_With_Sequence = '{col}' THEN columnValue END) AS {col}" for col in columns]
                    )
                }
        FROM sequenced_df
        WHERE Sequence IS NOT NULL
        GROUP BY recordId, Sequence
    )
    parsed_line AS (
        SELECT *,
            CAST(ORDER_D_NO AS INT) AS Item_Seq_Number,
            CASE 
                WHEN ORDER_D_FREE_ITEM31 = '' THEN NULL 
                ELSE ORDER_D_FREE_ITEM31 
            END AS Item_Type,
            CASE 
                WHEN ORDER_D_FREE_ITEM32 = '' THEN NULL 
                ELSE ORDER_D_FREE_ITEM32 
            END AS Barcode,
            CAST(TEIKA AS DOUBLE) AS Demand_Unit_Price,
            CAST(QUANTITY AS DOUBLE) AS Demand_Sales_Unit,
            CAST(TEIKA AS DOUBLE) * CAST(QUANTITY AS DOUBLE) AS Demand_Net,
            (CAST(SHIRE_PRICE AS DOUBLE) + 
                CASE 
                    WHEN ORDER_D_FREE_ITEM31 = 'C' THEN CAST(ORDER_D_FREE_ITEM35 AS DOUBLE) 
                    ELSE 0 
                END - CAST(TEIKA AS DOUBLE)) * CAST(QUANTITY AS DOUBLE) AS Discount_Amount,
            cast(NULL as string) AS Gift_IND,
            'N' AS Non_Merch_Item,
            'X' AS Cancelled_Status,
            'Cancelled' AS Cancelled_Status_Text,
            cast(NULL as string) AS Cancelled_Type
        FROM line
    )
    SELECT 
        header.eCom_Store_ID,
        header.Transaction_Nbr,
        header.Order_Date,
        COALESCE(item.Style, 'NULL') AS AX_Style_ID,
        COALESCE(item.Color, 'NULL') AS AX_Color_ID,
        COALESCE(item.Size, 'NULL') AS AX_Size_ID,
        parsed_line.Demand_Net,
        parsed_line.Demand_Sales_Unit,
        0 AS Shipment_ID,
        COALESCE(header.Shipment_Method, 'NULL') AS Shipment_Method,
        header.Ship_To_Country,
        0 AS Gift_IND,
        header.Gift_Charge AS GiftChrg,
        header.Gift_Tax AS GiftTax,
        parsed_line.Item_Seq_Number AS ItmSeqNum,
        parsed_line.Discount_Amount AS DiscountAmt
    FROM parsed_line
    INNER JOIN (SELECT * FROM parsed_header_df WHERE Cancel_Date IS NULL) AS header ON parsed_line.recordId = header.recordId
    LEFT JOIN item_mapping AS item ON parsed_line.Barcode = item.Barcode
""")

# Cancel final table
cancel_final = spark.sql("""
    SELECT 
        Order_Date,
        FORMAT_STRING('%.2f', Line_Total) AS Order_Total,
        header.eCom_Store_ID AS Store,
        COALESCE(header.Employee_ID, 'NULL') AS Empl_Id,
        header.Jda_Order,
        parsed_line.Item_Seq_Number AS Line,
        parsed_line.Cancelled_Status AS Status,
        parsed_line.Cancelled_Status_Text AS Status_Text,
        COALESCE(parsed_line.Cancelled_Type, 'NULL') AS Type,
        header.Transaction_Nbr AS Weborder_Id,
        header.Cancel_Date AS Status_Date,
        FORMAT_STRING('%.2f', header.Demand_Gross) AS Order_Demand_Amt,
        parsed_line.Barcode AS Upc,
        COALESCE(item.Style, 'NULL') AS APAC_style,
        COALESCE(item.Color, 'NULL') AS APAC_colour,
        COALESCE(item.Size, 'NULL') AS APAC_size,
        FORMAT_STRING('%.2f', parsed_line.Demand_Sales_Unit) AS Line_Qty,
        FORMAT_STRING('%.2f', parsed_line.Demand_Net) AS Line_Sub_Total,
        parsed_line.Discount_Amount AS Disc_Promo,
        Tender_ID AS Tender_1,
        COALESCE(NULL, 'NULL') AS Tender_2,
        COALESCE(NULL, 'NULL') AS Tender_3,
        COALESCE(NULL, 'NULL') AS Customer_Ip_Add,
        'Website' AS Order_Entry_Method,
        COALESCE(NULL, 'NULL') AS Csr_Associate_Name,
        header.Country AS Country_Code,
        header.eCom_Store_ID AS Demand_Location,
        COALESCE(NULL, 'NULL') AS Fulfilling_Location,
        header.Jda_Order AS Original_Jda_Order,
        COALESCE(NULL, 'NULL') AS Warehouse_Reason_Code,
        COALESCE(NULL, 'NULL') AS Warehouse_Reason,
        COALESCE(NULL, 'NULL') AS Customer_Reason_Code,
        COALESCE(NULL, 'NULL') AS Customer_Reason,
        header.Demand_Currency AS Currency,
        header.Country AS Webstore
    FROM parsed_header_df AS header
    INNER JOIN parsed_line ON header.recordId = parsed_line.recordId
    LEFT JOIN item_mapping AS item ON parsed_line.Barcode = item.Barcode
    WHERE header.Cancel_Date IS NOT NULL
""")

# Mandatory field check

from functools import reduce
mandatory_field_check_flag = "mandatory_field_check"

def check_mandatory_fields(df: DataFrame, mandatory_fields: list[str] = []):
    
    if mandatory_field_check_flag not in df.columns:
        df = df.withColumn(mandatory_field_check_flag, f.lit(True))
    df = df.withColumn(mandatory_field_check_flag, 
        f.when(f.col(mandatory_field_check_flag)==False, False)\
        .when(reduce(lambda x, y: x | y, [f.col(c).isNull() for c in mandatory_fields]), False) \
        .when(reduce(lambda x, y: x | y, [f.col(c) == f.lit('') for c in mandatory_fields]), False) \
        .otherwise(True)
    )
    return df



header_final = check_mandatory_fields(header_final, mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date", "Country", "Device_Type", "Demand_Net"])
payment_final = check_mandatory_fields(payment_final, mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
line_final = check_mandatory_fields(line_final, mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date", "AX_Style_ID", "AX_Color_ID", "AX_Size_ID", "Demand_Net", "Demand_Sales_Unit", "ItmSeqNum"])
cancel_final = check_mandatory_fields(cancel_final, mandatory_fields = ["Order_Date", "Store", "Weborder_Id", "Status_Date", "Order_Demand_Amt", "APAC_Style", "APAC_Colour", "APAC_Size", "Line_Qty", "Line_Sub_Total", "Country_Code"])


def write_to_delta_table(df, table_name, primary_keys: list[str], deduplicate=True):

    primary_keys = [f"source.{key} = target.{key}" for key in primary_keys]
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", f.current_timestamp())

    location = os.path.join(outbound_path, table_name)
    df.createOrReplaceTempView("temp_table")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ebisumart_{table_name}_history
        USING DELTA
        LOCATION '{location}/history'
        AS 
            SELECT * 
            FROM temp_table 
            WHERE 1 <> 1
    """)

    if deduplicate:
        # Write to history where the primary keys don't exist
        spark.sql(f"""
            INSERT INTO ebisumart_{table_name}_history
            SELECT *
            FROM temp_table AS source
            WHERE NOT EXISTS (
                SELECT 1
                FROM ebisumart_{table_name}_history AS target
                WHERE {' and '.join(primary_keys)}
            )
                AND {mandatory_field_check_flag} = True
        """)
    else:
        #  Write to history where the primary keys don't exist
        #  Or update if the primary keys exist
        spark.sql(f"""
            MERGE INTO ebisumart_{table_name}_history as source
            USING temp_table AS target
            WHERE source.{mandatory_field_check_flag} = True
            ON {' and '.join(primary_keys)}
            WHEN MATCHED AND source.{mandatory_field_check_flag} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    # Write to error where the primary keys is null or empty string
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS ebisumart_{table_name}_error
            USING DELTA
            LOCATION '{location}/error'
            AS 
                SELECT * 
                FROM temp_table 
                WHERE 1 <> 1
        """)
    spark.sql(f"""
        INSERT INTO ebisumart_{table_name}_error
        SELECT *
        FROM temp_table AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM ebisumart_{table_name}_history AS l
            WHERE {' and '.join(primary_keys)}
        )
            AND {mandatory_field_check_flag} = False
    """)

write_to_delta_table(header_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
write_to_delta_table(payment_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
write_to_delta_table(line_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
write_to_delta_table(cancel_final, ["Store", "Weborder_Id", "Order_Date"])


# Archive Inbound files
# Step 1: List the files in the inbound path
inbound_files = dbutils.fs.ls(jp_ebisumart_inbound_path)

# Step 2: Iterate over the file list
for file_info in inbound_files:
    # Check if the file has a .txt extension
    if file_info.name.lower().endswith(".txt"):
        # Construct the source and destination paths
        source_path = os.path.join(jp_ebisumart_inbound_path, file_info.name)
        archive_path = os.path.join(jp_ebisumart_archive_inbound_path,job_time.strftime("%Y/%m/%d"),file_info.name)
        # Archive the .txt file
        dbutils.fs.mv(source_path, archive_path)