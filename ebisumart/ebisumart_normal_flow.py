# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for processing Normal flow of eCom KPI Demand from Ebisumart
# MAGIC Data includes
# MAGIC - Demand headers
# MAGIC - Demand lines
# MAGIC - Demand payments
# MAGIC - Cancel lines
# MAGIC
# MAGIC External dependencies
# MAGIC - ~~Maven coordinate `com.crealytics:spark-excel_2.12:3.4.1_0.20.3` for reading/writing Excel files (requires to update the version if the Spark version is different)~~
# MAGIC - None

# COMMAND ----------

# MAGIC %md
# MAGIC # Import library

# COMMAND ----------

from datetime import datetime, timezone, timedelta
import os
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructType, StructField

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_normal_path_prefix: str = "EcomKPI/Demand/Normal"
demand_mapping_path_prefix: str = "EcomKPI/Demand/Mapping"
jp_ebisumart_inbound_path: str = os.path.join(demand_normal_path_prefix, "Inbound/ebisumart/JP")
jp_ebisumart_outbound_temp_path: str = os.path.join(demand_normal_path_prefix, "Outbound/temp/ebisumart/JP")
jp_ebisumart_outbound_path: str = os.path.join(demand_normal_path_prefix, "Outbound/ebisumart/JP")
jp_ebisumart_archive_inbound_path: str = os.path.join(demand_normal_path_prefix, "Archive/Inbound/ebisumart/JP")
jp_ebisumart_archive_outbound_path: str = os.path.join(demand_normal_path_prefix, "Archive/Outbound/ebisumart/JP")
jp_ebisumart_tender_mapping_path: str = os.path.join(demand_mapping_path_prefix, "EbisumartTenderMapping.csv")
job_time: datetime = datetime.now(timezone(timedelta(hours=8)))


azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tender Mapping

# COMMAND ----------

# Read Tender Mapping
ebisumart_tender_schema: StructType = StructType([
    StructField("TenderID", StringType(), False),
    StructField("TenderType", StringType(), False)
])
jp_ebisumart_tender_mapping_azure_path = azure_storage_path_format.format(jp_ebisumart_tender_mapping_path)
ebisumart_tender_mapping_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(ebisumart_tender_schema).load(jp_ebisumart_tender_mapping_azure_path)
ebisumart_tender_mapping_df.createOrReplaceTempView("tender_mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Item Mapping

# COMMAND ----------

# Read Item Mapping
item_mapping_query: str = """
(SELECT product.THK_EANCODE AS "Barcode", product.PRODUCTMASTERNUMBER AS "Style", product.PRODUCTCOLORID AS "Color", product.PRODUCTSIZEID AS "Size"
FROM [dbo].[EcoResProductVariantStaging] product WITH (NOLOCK)
WHERE product.PRODUCTCONFIGURATIONID = 'A') temp
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
ebisumart_raw_path = os.path.join(azure_storage_path_format.format(jp_ebisumart_inbound_path), "*.txt")
ebisumart_raw_df: DataFrame = spark.read.format("text")\
    .options(
        wholetext=False, 
        lineSep="\n",
    )\
    .load(ebisumart_raw_path)
ebisumart_raw_df.createOrReplaceTempView("ebisumart_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data cleaning

# COMMAND ----------

# Step 0: Add record id and decode strings
spark.sql("""
    CREATE OR REPLACE TEMP VIEW ebisumart_decoded_df AS
    SELECT 
        UUID() AS recordId, 
        value AS rawString, 
        url_decode(value) AS decodedString 
    FROM ebisumart_raw
""")

# Step 1: Split the decodedString and explode the array
spark.sql("""
    CREATE OR REPLACE TEMP VIEW split_df AS
    SELECT *,
        SPLIT(decodedString, '&') AS splitDecodedString
    FROM ebisumart_decoded_df
""")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW exploded_df AS
    SELECT *,
        EXPLODE(splitDecodedString) AS arrayToRow
    FROM split_df
""")

# Step 2: Convert to rows and extract column names and values
spark.sql("""
    CREATE OR REPLACE TEMP VIEW converted_to_rows_df AS
    SELECT recordId,
        SPLIT_PART(arrayToRow, '=', 1) AS columnName,
        SPLIT_PART(arrayToRow, '=', 2) AS columnValue
    FROM exploded_df
""")

# Step 3: Add sequence and column name with sequence
spark.sql("""
    CREATE OR REPLACE TEMP VIEW sequenced_df AS
    SELECT *,
        CASE WHEN REGEXP_LIKE(columnName, '(\\d+)$') THEN REGEXP_EXTRACT(columnName, '(\\d+)$', 1) END AS Sequence,
        CASE WHEN REGEXP_LIKE(columnName, '^(.*?)(?:\\d+)$') THEN REGEXP_EXTRACT(columnName, '^(.*?)(?:\\d+)$', 1) END AS Column_Name_With_Sequence
    FROM converted_to_rows_df
""")

# Step 4: Pivot (Get the first value for each column for each recordId)
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
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW header AS
    SELECT *
          FROM 
            (SELECT recordId, columnName, columnValue from sequenced_df)
          PIVOT
            (first(columnValue) FOR columnName in ({pivot_columns}))
""")

# Step 5: Transform header table
spark.sql("""
CREATE OR REPLACE TEMP VIEW parsed_header_df AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Header

# COMMAND ----------

# Header final table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW header_final AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment

# COMMAND ----------

# Payment final table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW payment_final AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Line

# COMMAND ----------

# Line table
columns = [
    "ORDER_D_NO", "ITEM_NAME", "ITEM_ITEMPROPERTY_CD", "ORDER_D_FREE_ITEM1",
    "ORDER_D_FREE_ITEM2", "ORDER_D_FREE_ITEM3", "ORDER_D_FREE_ITEM4",
    "ORDER_D_FREE_ITEM5", "ORDER_D_FREE_ITEM31", "ORDER_D_FREE_ITEM32",
    "ORDER_D_FREE_ITEM35", "ORDER_D_FREE_ITEM36", "ORDER_D_FREE_ITEM37",
    "ORDER_D_FREE_ITEM38", "OUTPUT_FLG", "QUANTITY", "SHIRE_PRICE",
    "TEIKA", "MEMBER_WARIBIKI", "ADD_POINT"
]
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW line AS
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
""")
# Build parsed line
spark.sql("""
    CREATE OR REPLACE TEMP VIEW parsed_line AS
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
""")

# Line final table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW line_final AS
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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cancel

# COMMAND ----------

# Cancel final table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW cancel_final AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Mandatory field check

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function definition

# COMMAND ----------

from functools import reduce
flag_column = "mandatory_field_check"

def check_mandatory_fields(df: DataFrame, mandatory_fields: list[str] = []):
    
    if flag_column not in df.columns:
        df = df.withColumn(flag_column, f.lit(True))
    df = df.withColumn(flag_column, 
        f.when(f.col(flag_column)==False, False)\
        .when(reduce(lambda x, y: x | y, [f.col(c).isNull() for c in mandatory_fields]), False) \
        .when(reduce(lambda x, y: x | y, [f.col(c) == f.lit('') for c in mandatory_fields]), False) \
        .otherwise(True)
    )
    return df

def verify_table(table_name, mandatory_fields):
    df = spark.table(f"{table_name}_final")
    df = check_mandatory_fields(df, mandatory_fields)
    df.createOrReplaceTempView(f"{table_name}_final_validated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate tables

# COMMAND ----------

verify_table("header", mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date", "Country", "Device_Type", "Demand_Net"])
verify_table("payment", mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
verify_table("line", mandatory_fields=["eCom_Store_ID", "Transaction_Nbr", "Order_Date", "AX_Style_ID", "AX_Color_ID", "AX_Size_ID", "Demand_Net", "Demand_Sales_Unit", "ItmSeqNum"])
verify_table("cancel", mandatory_fields = ["Order_Date", "Store", "Weborder_Id", "Status_Date", "Order_Demand_Amt", "APAC_Style", "APAC_Colour", "APAC_Size", "Line_Qty", "Line_Sub_Total", "Country_Code"])


# COMMAND ----------

# MAGIC %md
# MAGIC # Check for previous record sent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function definition

# COMMAND ----------

def filter_previous_record_sent(table_name, primary_keys):
    def create_log_table(table_name):
        location = azure_storage_path_format.format(
            os.path.join(
                jp_ebisumart_outbound_path,
                "log",
                table_name
            )
        )
        spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_log
                USING DELTA
                LOCATION '{location}'
                AS SELECT * FROM {table_name}_final_validated where 1<>1
        """)

    def exclude_sent_records(table_name, primary_keys):
        primary_keys = [f"s.{pk} = l.{pk}" for pk in primary_keys]
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW ebisumart_{table_name}_final_validated_dedup 
            AS
                SELECT *
                FROM {table_name}_final_validated AS s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {table_name}_log AS l
                    WHERE {' and '.join(primary_keys)}
                )
        """)

    def insert_to_log_table(table_name):
        spark.sql(f"INSERT INTO ebisumart_{table_name}_log SELECT * FROM {table_name}_final_validated_dedup ")

    create_log_table(table_name)
    exclude_sent_records(table_name, primary_keys)
    insert_to_log_table(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter sent data

# COMMAND ----------

filter_previous_record_sent("header", ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
filter_previous_record_sent("payment", ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
filter_previous_record_sent("line", ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"])
filter_previous_record_sent("cancel", ["Store", "Weborder_Id", "Order_Date"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions definition

# COMMAND ----------

current_time = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
output_path_template = os.path.join(
    azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
    "{{0}}s",
    "JPEcom_KPI_Ebisumart_{{0}}_{0}".format(current_time)
)

from uuid import uuid4
def export_to_csv(df, path):
    temp_path = azure_storage_path_format.format(f"tmp/{uuid4()}")
    df \
        .coalesce(1) \
        .write \
        .format("csv") \
        .options(**{
            "sep": ",",
            "lineSep": "\r\n",
            "encoding": "UTF-8",
            "quote": "",
            "escape": "",
            "header": True,
            "multiLine": False
        }) \
        .mode("overwrite") \
        .save(temp_path)
    output_files = dbutils.fs.ls(temp_path)
    output_files = [file for file in output_files if file.name.endswith(".csv")]
    if len(output_files) > 1:
        raise Exception(f"Multiple files found in temp path \"{temp_path}\". Please ensure Spark only write to 1 CSV file.")
    dbutils.fs.mv(output_files[0].path, path)
    dbutils.fs.rm(temp_path, recurse=True)
    

min_order_date = spark.sql("SELECT MIN(Order_Date) AS date FROM parsed_header_df").collect()[0]["date"]
if min_order_date is None:
    raise TypeError("Min order date is NoneType in parsed_header_df.")
min_order_date: datetime = datetime.strptime(min_order_date,"%Y-%m-%d %H:%M:%S")

def write_table_to_pending(table_name, file_type):
    # Read final and validated data
    df = spark.table(f"{table_name}_final_validated")
    # Write correct data to pending
    correct_df = df.filter(f"{flag_column} == True")
    outbound_path = azure_storage_path_format.format(
        os.path.join(
            jp_ebisumart_outbound_path,
            f"pending_to_send",
            f"{table_name}s",
            f"BI-RalphLauren_{'JP'}_{file_type}_{min_order_date.strftime('%Y%m%d')}.csv"
        )
    )
    export_to_csv(correct_df, outbound_path)
    # Write error data to error, for send email
    error_df = df.filter(f"{flag_column} == False")
    if error_df.count() > 0:
        error_path = azure_storage_path_format.format(
            os.path.join(
                jp_ebisumart_outbound_path,
                f"error",
                f"{table_name}s",
                f"BI-RalphLauren_{'JP'}_{file_type}_{min_order_date.strftime('%Y%m%d')}_error.csv"
            )
        )
        export_to_csv(error_df, error_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Pending_to_send

# COMMAND ----------

write_table_to_pending("header", "Transaction")
write_table_to_pending("payment", "Payment")
write_table_to_pending("line", "Line")
write_table_to_pending("cancel", "Cancel")