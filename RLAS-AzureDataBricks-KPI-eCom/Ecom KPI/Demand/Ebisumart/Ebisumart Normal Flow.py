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

from datetime import datetime, timezone, timedelta
import os
from typing import Dict, List
from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_types

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_normal_path_prefix: str = "EcomKPI/Demand/Normal"
demand_mapping_path_prefix: str = "EcomKPI/Demand/Mapping"
jp_ebisumart_inbound_path: str = os.path.join(demand_normal_path_prefix, "Inbound/ebisumart/JP")
# jp_ebisumart_inbound_decoded_path: str = os.path.join(jp_ebisumart_inbound_path, "Decoded")
jp_ebisumart_outbound_temp_path: str = os.path.join(demand_normal_path_prefix, "Outbound/temp/ebisumart/JP")
jp_ebisumart_outbound_path: str = os.path.join(demand_normal_path_prefix, "Outbound/ebisumart/JP")
jp_ebisumart_archive_inbound_path: str = os.path.join(demand_normal_path_prefix, "Archive/Inbound/ebisumart/JP")
jp_ebisumart_archive_outbound_path: str = os.path.join(demand_normal_path_prefix, "Archive/Outbound/ebisumart/JP")
jp_ebisumart_tender_mapping_path: str = os.path.join(demand_mapping_path_prefix, "EbisumartTenderMapping.csv")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

@spark_functions.udf(returnType=spark_types.StringType())
def udf_decode_www_form_urlencoded_column(uri: str) -> str:
    import urllib.parse

    return urllib.parse.unquote_plus(uri)

if spark.version >= "3.5.0":
    spark_udf_decode_www_form_urlencoded_column = spark_functions.url_decode
else:
    spark_udf_decode_www_form_urlencoded_column = udf_decode_www_form_urlencoded_column

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

ebisumart_tender_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("TenderID", spark_types.StringType(), False),
    spark_types.StructField("TenderType", spark_types.StringType(), False)
])

ebisumart_tender_mapping_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(ebisumart_tender_schema).load(azure_storage_path_format.format(jp_ebisumart_tender_mapping_path))

# dbutils.fs.cp(azure_storage_path_format.format(jp_ebisumart_tender_mapping_path), "file:///tmp/ecom/kpi/demand/normal/ebisumart")
# ebisumart_tender_mapping_df: DataFrame = spark.createDataFrame(pd.read_csv("/tmp/ecom/kpi/demand/normal/ebisumart"), schema=ebisumart_tender_schema)

# COMMAND ----------

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

# # secondary_db_jdbc_url_format: str = "jdbc:sqlserver://{0}:{1};database={2}"
# secondary_db_options = {
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#     # "url": secondary_db_jdbc_url_format.format("", "1433", ""),
#     "url": dbutils.secrets.get("storagescope", "rlas-SQLBI-ConnString-PRD"),
#     "dbtable": item_mapping_query,
#     # "user": "",
#     # "password": "",
#     "numPartitions": 4
# }
# item_mapping_df: DataFrame = spark.read.format("jdbc").options(**secondary_db_options).load()

# COMMAND ----------

os.path.join(azure_storage_path_format.format(jp_ebisumart_inbound_path), "*.txt")

# COMMAND ----------

ebisumart_raw_df: DataFrame = spark.read.format("text").options(**{
    "wholetext": False,
    "lineSep": "\n"
}).load(os.path.join(azure_storage_path_format.format(jp_ebisumart_inbound_path), "*.txt"))

ebisumart_raw_df = (ebisumart_raw_df.withColumnRenamed("value", "rawString")
    .withColumn("recordId", spark_functions.expr("uuid()"))
    .select("recordId", "rawString")
)

# COMMAND ----------

ebisumart_decoded_df: DataFrame = ebisumart_raw_df.withColumn("decodedString", spark_udf_decode_www_form_urlencoded_column("rawString"))

# COMMAND ----------

split_df: DataFrame = ebisumart_decoded_df.withColumn("splitDecodedString", spark_functions.split("decodedString", r"&"))
exploded_df: DataFrame = split_df.withColumn("arrayToRow", spark_functions.explode("splitDecodedString"))

# COMMAND ----------

converted_to_rows_df: DataFrame = (exploded_df
    .withColumn("columnName", spark_functions.split_part("arrayToRow", spark_functions.lit(r"="), spark_functions.lit(1)))
    .withColumn("columnValue", spark_functions.split_part("arrayToRow", spark_functions.lit(r"="), spark_functions.lit(2)))
).select("recordId", "columnName", "columnValue")

# COMMAND ----------

sequenced_df: DataFrame = (converted_to_rows_df
    .withColumn("_Sequence", spark_functions.when(spark_functions.regexp_like("columnName", spark_functions.lit(r"_(\d+)$")), spark_functions.regexp_extract("columnName", r"_(\d+)$", 1)))
    .withColumn("_Column_Name_With_Sequence", spark_functions.when(spark_functions.regexp_like("columnName", spark_functions.lit(r"^(.+?)_(?:\d+)$")), spark_functions.regexp_extract("columnName", r"^(.+?)_(?:\d+)$", 1)))
)

# COMMAND ----------

header_df: DataFrame = sequenced_df.groupBy("recordId").pivot("columnName", ["ADDR1", "ADDR2", "ADDR3", "ADD_POINT_SUM", "AUTO_CANCEL_MAIL_DATE", "BIKO", "CANCEL_DATE", "CANCEL_MAIL_DATE", "COUPON_ID", "COUPON_WARIBIKI", "CREDIT_COUNT", "CREDIT_KIND", "CREDIT_LIMIT", "CREDIT_NAME", "CREDIT_NO", "DAIBIKI", "DAIBIKI_TAX", "DEL_DATE", "DOWNLOAD_DATE", "FREE_ITEM1", "FREE_ITEM2", "FREE_ITEM3", "FREE_ITEM4", "FREE_ITEM5", "FREE_ITEM6", "FREE_ITEM7", "FREE_ITEM8", "FREE_ITEM9", "FREE_ITEM10", "FREE_ITEM11", "FREE_ITEM12", "FREE_ITEM13", "FREE_ITEM14", "FREE_ITEM15", "FREE_ITEM33", "FREE_ITEM34", "FREE_ITEM35", "FREE_ITEM36", "F_KANA", "F_NAME", "HAISO_ID", "HARAIKOMI_URL", "ITEM_CD", "KESSAI_ID", "L_KANA", "L_NAME", "MEMBER_ID", "MEMBER_WARIBIKI_SUM", "MOBILE_MAIL", "NETMILE_ADD_POINT_ALL", "ORDER_DATE", "ORDER_DISP_NO", "ORDER_D_FREE_ITEM31_1", "ORDER_H_ACCESS_BROWSER_KBN", "ORDER_MAIL_DATE", "ORDER_NO", "PAYMENT_DATE", "PAYMENT_MONEY", "PAY_LIMIT", "PC_MAIL", "PC_MOBILE_KBN", "POINT_USE", "POINT_WARIBIKI", "RECEIPT_NO", "REMINDER_MAIL_DATE", "SEIKYU", "SEND_ADDR1", "SEND_ADDR2", "SEND_ADDR3", "SEND_DATE", "SEND_F_KANA", "SEND_F_NAME", "SEND_HOPE_DATE", "SEND_HOPE_TIME", "SEND_L_KANA", "SEND_L_NAME", "SEND_MAIL", "SEND_MAIL_DATE", "SEND_TEL", "SEND_ZIP", "SESSION_ID", "SHIP_SLIP_NO", "SHIRE_SUM", "SORYO", "SORYO_TAX", "SYOUKAI_NO", "TAX", "TEIKA_SUM", "TEIKA_SUM_TAX", "TEL", "USER_AGENT", "VERI_TORIHIKI_ID", "ZIP"]).agg({
    "columnValue": "first"
})

# COMMAND ----------

parsed_header_df: DataFrame = (header_df
      .withColumn(
            "Item_Type",
            spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31_1") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                  .otherwise(spark_functions.col("ORDER_D_FREE_ITEM31_1"))
      )
      .withColumn(
            "eCom_Store_ID",
            spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.lit("57037"))
                  .when(spark_functions.col("Item_Type") == spark_functions.lit("L"), spark_functions.lit("56001"))
                  .otherwise(spark_functions.lit("57036"))
      )
      .withColumn("Transaction_Nbr", spark_functions.regexp_replace("ORDER_DISP_NO", spark_functions.lit(r"^(?:ORDER|e)"), spark_functions.lit("")))
      .withColumn(
            "Order_Date",
            spark_functions.date_format(
                  spark_functions.to_timestamp(spark_functions.col("ORDER_DATE"), "yyyy/MM/dd"),
                  "yyyy-MM-dd HH:mm:ss"
            )
      )
      .withColumn(
            "Cancel_Date",
            spark_functions.date_format(
                  spark_functions.to_timestamp(spark_functions.col("CANCEL_DATE"), "yyyy/MM/dd"),
                  "yyyy-MM-dd HH:mm:ss"
            )
      )
      .withColumn("Country", spark_functions.lit("JP"))
      .withColumn("Device_Type_ID", spark_functions.left(spark_functions.col("ORDER_H_ACCESS_BROWSER_KBN"), spark_functions.lit(1)).cast(spark_types.IntegerType()))
      .withColumn(
            "Device_Type",
            spark_functions.when(spark_functions.col("Device_Type_ID").between(1, 4),
                              spark_functions.element_at(spark_functions.lit(["DESKTOP", "MOBILE", "MOBILE PHONE", "TABLET"]), spark_functions.col("Device_Type_ID")))
                  .otherwise(spark_functions.lit("OTHER"))
      )
      .withColumn("Employee_Purchase", spark_functions.lit(None).cast(spark_types.StringType()))
      .withColumn("Order_Channel", spark_functions.lit("Website"))
      .withColumn(
            "Customer_ID",
            spark_functions.when(spark_functions.col("MEMBER_ID") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                  .otherwise(spark_functions.col("MEMBER_ID"))
      )
      .withColumn(
            "Shipment_ID",
            spark_functions.when(spark_functions.col("SHIP_SLIP_NO") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                  .otherwise(spark_functions.col("SHIP_SLIP_NO"))
      )
      .withColumn("Shipment_Method", spark_functions.lit(None).cast(spark_types.StringType()))
      .withColumn("Ship_To_Country", spark_functions.lit("JP"))
      .withColumn("Ship_To", spark_functions.lit(None).cast(spark_types.StringType()))
      .withColumn("Bill_To", spark_functions.lit(None).cast(spark_types.StringType()))
      .withColumn(
            "Promotion_Coupons",
            spark_functions.when(spark_functions.coalesce(spark_functions.col("COUPON_ID"), spark_functions.lit("")) == spark_functions.lit(""), spark_functions.lit("N"))
                  .otherwise(spark_functions.lit("Y"))
      )
      .withColumn("Freight_Tax", spark_functions.col("SORYO_TAX").cast(spark_types.DoubleType()))
      .withColumn("Demand_Gross", spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.col("TEIKA_SUM_TAX").cast(spark_types.DoubleType()))
      #.withColumn("Demand_Net", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()) - spark_functions.col("TAX").cast(spark_types.#DoubleType()) - spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) + spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) #- spark_functions.col("FREE_ITEM11").cast(spark_types.DoubleType()))
      .withColumn("Demand_Net", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()) - spark_functions.col("TAX").cast(spark_types.DoubleType()) - spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) - spark_functions.col("SORYO").cast(spark_types.DoubleType()) - spark_functions.col("COUPON_WARIBIKI").cast(spark_types.DoubleType()) - spark_functions.col("FREE_ITEM11").cast(spark_types.DoubleType()))
      .withColumn("Employee_ID", spark_functions.lit(None).cast(spark_types.StringType()))
      .withColumn("Bill_To_Country", spark_functions.lit("JP"))
      .withColumn("Demand_Currency", spark_functions.lit("JPY"))
      .withColumn("Freight_Amount", spark_functions.col("SORYO").cast(spark_types.DoubleType()))
      .withColumn(
            "Discount_Amount",
            spark_functions.col("SHIRE_SUM").cast(spark_types.DoubleType()) - spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31_1") == spark_functions.lit("C"), spark_functions.col("FREE_ITEM33").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType()))
      )
      .withColumn("Coupon_Amount", spark_functions.col("COUPON_WARIBIKI").cast(spark_types.DoubleType()))
      .withColumn("Tax", spark_functions.col("TAX").cast(spark_types.DoubleType()))
      .withColumn("CoD_Charge", spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()))
      .withColumn("CoD_Charge_Tax", spark_functions.col("DAIBIKI_TAX").cast(spark_types.DoubleType()))
      .withColumn("Shipping_Fee", spark_functions.col("SORYO").cast(spark_types.DoubleType()))
      .withColumn("Shipping_Fee_Tax", spark_functions.col("SORYO_TAX").cast(spark_types.DoubleType()))
      .withColumn("Line_Total", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()))
      .withColumn("Tender_ID", spark_functions.col("KESSAI_ID"))
      .withColumn("Tender_Amt", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()))
      .withColumn("Gift_Charge", spark_functions.col("FREE_ITEM11").cast(spark_types.DoubleType()))
      .withColumn("Gift_Tax", spark_functions.col("Gift_Charge") * spark_functions.lit(0.1))
      .withColumn("Jda_Order", spark_functions.lit(-1))
).select("recordId", "eCom_Store_ID", "Transaction_Nbr", "Order_Date", "Cancel_Date", "Country", "Device_Type", "Employee_Purchase", "Order_Channel", "Customer_ID", "Shipment_ID", "Shipment_Method", "Ship_To_Country", "Ship_To", "Bill_To", "Promotion_Coupons", "Freight_Tax", "Demand_Gross", "Demand_Net", "Employee_ID", "Bill_To_Country", "Demand_Currency", "Freight_Amount", "Discount_Amount", "Coupon_Amount", "Tax", "CoD_Charge", "CoD_Charge_Tax", "Shipping_Fee", "Shipping_Fee_Tax", "Line_Total", "Tender_ID", "Tender_Amt", "Item_Type", "Gift_Charge", "Gift_Tax", "Jda_Order")

# COMMAND ----------

demand_header_df: DataFrame = parsed_header_df.filter("Cancel_Date IS NULL")
data_date: datetime = datetime.strptime(
    parsed_header_df.selectExpr("MIN(Order_Date) AS date").collect()[0]["date"],
    "%Y-%m-%d %H:%M:%S"
)
demand_header_final_df: DataFrame = demand_header_df.selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "Country",
    "Device_Type",
    "COALESCE(Employee_Purchase, 'NULL') AS Employee_Purchase",
    "Order_Channel",
    "COALESCE(Customer_ID, 'NULL') AS Customer_ID",
    "COALESCE(Ship_To, 'NULL') AS Ship_To",
    "COALESCE(Bill_To, 'NULL') AS Bill_To",
    "Promotion_Coupons",
    "Freight_Tax",
    "Demand_Net",
    "COALESCE(Employee_ID, 'NULL') AS Employee_ID",
    "Bill_To_Country",
    "Freight_Amount",
    "Discount_Amount",
    "Coupon_Amount",
    "Tax"
    # "CoD_Charge",
    # "CoD_Charge_Tax",
    # "Shipping_Fee",
    # "Shipping_Fee_Tax",
    # "Line_Total"
)

demand_payment_df: DataFrame = parsed_header_df.filter("Cancel_Date IS NULL")
demand_payment_final_df: DataFrame = demand_payment_df.alias("payment").join(ebisumart_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("payment.Tender_ID") == spark_functions.col("tender.TenderID")]).selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "TenderType AS Tender",
    "Tender_Amt",
    "Demand_Currency"
)

# COMMAND ----------

(
    demand_header_final_df
        .coalesce(1)
        .write
        .format("csv")
        .options(**{
            "sep": ",",
            "lineSep": "\r\n",
            "encoding": "UTF-8",
            "quote": "",
            "escape": "",
            "header": True,
            "multiLine": False
        })
        .mode("overwrite")
        .save(
            os.path.join(
                azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
                "headers",
                "JPEcom_KPI_Ebisumart_Transaction_{0}".format(datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S"))
            )
        )
)

# COMMAND ----------

(
    demand_payment_final_df
        .coalesce(1)
        .write
        .format("csv")
        .options(**{
            "sep": ",",
            "lineSep": "\r\n",
            "encoding": "UTF-8",
            "quote": "",
            "escape": "",
            "header": True,
            "multiLine": False
        })
        .mode("overwrite")
        .save(
            os.path.join(
                azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
                "payments",
                "JPEcom_KPI_Ebisumart_Payment_{0}".format(datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S"))
            )
        )
)

# COMMAND ----------

line_df: DataFrame = sequenced_df.filter("_Sequence IS NOT NULL").groupBy("recordId", "_Sequence").pivot("_Column_Name_With_Sequence", ["ORDER_D_NO", "ITEM_NAME", "ITEM_ITEMPROPERTY_CD", "ORDER_D_FREE_ITEM1", "ORDER_D_FREE_ITEM2", "ORDER_D_FREE_ITEM3", "ORDER_D_FREE_ITEM4", "ORDER_D_FREE_ITEM5", "ORDER_D_FREE_ITEM31", "ORDER_D_FREE_ITEM32", "ORDER_D_FREE_ITEM35", "ORDER_D_FREE_ITEM36", "ORDER_D_FREE_ITEM37", "ORDER_D_FREE_ITEM38", "OUTPUT_FLG", "QUANTITY", "SHIRE_PRICE", "TEIKA", "MEMBER_WARIBIKI", "ADD_POINT"]).agg({
    "columnValue": "first"
})

# COMMAND ----------

parsed_line_df: DataFrame = (line_df
    .withColumn("Item_Seq_Number", spark_functions.col("ORDER_D_NO").cast(spark_types.IntegerType()))
    .withColumn(
        "Item_Type",
        spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
            .otherwise(spark_functions.col("ORDER_D_FREE_ITEM31"))
    )
    .withColumn(
        "Barcode",
            spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM32") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                .otherwise(spark_functions.col("ORDER_D_FREE_ITEM32"))
    )
    .withColumn("Demand_Unit_Price", spark_functions.col("TEIKA").cast(spark_types.DoubleType()))
    .withColumn("Demand_Sales_Unit", spark_functions.col("QUANTITY").cast(spark_types.DoubleType()))
    .withColumn("Demand_Net", spark_functions.col("Demand_Unit_Price") * spark_functions.col("Demand_Sales_Unit"))
    .withColumn("Discount_Amount", (spark_functions.col("SHIRE_PRICE").cast(spark_types.DoubleType()) + spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.col("ORDER_D_FREE_ITEM35").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType())) - spark_functions.col("Demand_Unit_Price")) * spark_functions.col("Demand_Sales_Unit"))
    .withColumn("Gift_IND", spark_functions.lit(None).cast(spark_types.StringType()))
    .withColumn("Non_Merch_Item", spark_functions.lit("N"))
    .withColumn("Cancelled_Status", spark_functions.lit("X"))
    .withColumn("Cancelled_Status_Text", spark_functions.lit("Cancelled"))
    .withColumn("Cancelled_Type", spark_functions.lit(None).cast(spark_types.StringType()))
)

# COMMAND ----------

demand_line_final_df: DataFrame = (parsed_line_df.alias("line")
                                         .join(demand_header_df.alias("header"), how="inner", on=[spark_functions.col("line.recordId") == spark_functions.col("header.recordId")])
                                         .join(item_mapping_df.alias("item"), how="left", on=[spark_functions.col("line.Barcode") == spark_functions.col("item.Barcode")])
).selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "COALESCE(item.Style, 'NULL') AS AX_Style_ID",
    "COALESCE(item.Color, 'NULL') AS AX_Color_ID",
    "COALESCE(item.Size, 'NULL') AS AX_Size_ID",
    "line.Demand_Net",
    "Demand_Sales_Unit",
    # "COALESCE(Shipment_ID, 'NULL') AS Shipment_ID",
    "0 AS Shipment_ID",
    "COALESCE(Shipment_Method, 'NULL') AS Shipment_Method",
    "Ship_To_Country",
    # "COALESCE(Gift_IND, 'NULL') AS Gift_IND",
    "0 AS Gift_IND",
    "Non_Merch_Item",
    "Gift_Charge AS GiftChrg",
    "Gift_Tax AS GiftTax",
    "Item_Seq_Number AS ItmSeqNum",
    "line.Discount_Amount AS DiscountAmt"
)

# COMMAND ----------

(
    demand_line_final_df
        .coalesce(1)
        .write
        .format("csv")
        .options(**{
            "sep": ",",
            "lineSep": "\r\n",
            "encoding": "UTF-8",
            "quote": "",
            "escape": "",
            "header": True,
            "multiLine": False
        })
        .mode("overwrite")
        .save(
            os.path.join(
                azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
                "lines",
                "JPEcom_KPI_Ebisumart_Line_{0}".format(datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S"))
            )
        )
)

# COMMAND ----------

cancel_header_df: DataFrame = parsed_header_df.filter("Cancel_Date IS NOT NULL")
cancel_header_final_df: DataFrame = (cancel_header_df.alias("header")
                                     .join(parsed_line_df.alias("line"), how="inner", on=[spark_functions.col("header.recordId") == spark_functions.col("line.recordId")])
                                     .join(item_mapping_df.alias("item"), how="left", on=[spark_functions.col("line.Barcode") == spark_functions.col("item.Barcode")])
).selectExpr(
    "Order_Date",
    "FORMAT_STRING('%.2f', Line_Total) AS Order_Total",
    "eCom_Store_ID AS Store",
    "COALESCE(Employee_ID, 'NULL') AS Empl_Id",
    "Jda_Order",
    "Item_Seq_Number AS Line",
    "Cancelled_Status AS Status",
    "Cancelled_Status_Text AS Status_Text",
    "COALESCE(Cancelled_Type, 'NULL') AS Type",
    "Transaction_Nbr AS Weborder_Id",
    "Cancel_Date AS Status_Date",
    "FORMAT_STRING('%.2f', header.Demand_Gross) AS Order_Demand_Amt",
    "line.Barcode AS Upc",
    "COALESCE(item.Style, 'NULL') AS APAC_style",
    "COALESCE(item.Color, 'NULL') AS APAC_colour",
    "COALESCE(item.Size, 'NULL') AS APAC_size",
    "FORMAT_STRING('%.2f', line.Demand_Sales_Unit) AS Line_Qty",
    "FORMAT_STRING('%.2f', line.Demand_Net) AS Line_Sub_Total",
    "line.Discount_Amount AS Disc_Promo",
    "Tender_ID AS Tender_1",
    "COALESCE(NULL, 'NULL') AS Tender_2",
    "COALESCE(NULL, 'NULL') AS Tender_3",
    "COALESCE(NULL, 'NULL') AS Customer_Ip_Add",
    "'Website' AS Order_Entry_Method",
    "COALESCE(NULL, 'NULL') AS Csr_Associate_Name",
    "Country AS Country_Code",
    "eCom_Store_ID AS Demand_Location",
    "COALESCE(NULL, 'NULL') AS Fulfilling_Location",
    "Jda_Order AS Original_Jda_Order",
    "COALESCE(NULL, 'NULL') AS Warehouse_Reason_Code",
    "COALESCE(NULL, 'NULL') AS Warehouse_Reason",
    "COALESCE(NULL, 'NULL') AS Customer_Reason_Code",
    "COALESCE(NULL, 'NULL') AS Customer_Reason",
    "Demand_Currency AS Currency",
    "Country AS Webstore"
)

# COMMAND ----------

(
    cancel_header_final_df
        .coalesce(1)
        .write
        .format("csv")
        .options(**{
            "sep": "|",
            "lineSep": "\r\n",
            "encoding": "UTF-8",
            "quote": "",
            "escape": "",
            "header": True,
            "multiLine": False
        })
        .mode("overwrite")
        .save(
            os.path.join(
                azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
                "cancels",
                "JPEcom_KPI_Ebisumart_Cancel_{0}".format(datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S"))
            )
        )
)

# COMMAND ----------

inbound_file_list = dbutils.fs.ls(azure_storage_path_format.format(jp_ebisumart_inbound_path))
for i in range(len(inbound_file_list)):
    # Delete all TXT file in non-archive inbound path.
    if os.path.splitext(inbound_file_list[i].name)[-1].lower() == ".txt":
        # Archive the files.
        dbutils.fs.cp(
            azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_path, inbound_file_list[i].name)),
            azure_storage_path_format.format(os.path.join(jp_ebisumart_archive_inbound_path, job_time.strftime("%Y/%m/%d"), inbound_file_list[i].name))
        )

        # Delete all TXT file in non-archive inbound path.
        dbutils.fs.rm(
           azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_path, inbound_file_list[i].name))
        )

# inbound_decoded_file_list = dbutils.fs.ls(azure_storage_path_format.format(jp_ebisumart_inbound_decoded_path))
# for i in range(len(inbound_decoded_file_list)):
#     # Delete all TXT file in non-archive inbound decoded path.
#     if os.path.splitext(inbound_decoded_file_list[i].name)[-1].lower() == ".txt":
#         dbutils.fs.rm(
#             azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_decoded_path, inbound_decoded_file_list[i].name))
#         )

del i

print("Completed archiving and removing files in Inbound folder")

# COMMAND ----------

outbound_temp_type_folder_list = dbutils.fs.ls(azure_storage_path_format.format(jp_ebisumart_outbound_temp_path))

for i in range(len(outbound_temp_type_folder_list)):
    outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
    outbound_temp_date_folder_list = dbutils.fs.ls(
        azure_storage_path_format.format(os.path.join(jp_ebisumart_outbound_temp_path, outbound_temp_type_folder_name))
    )

    file_data_type: str
    match outbound_temp_type_folder_name:
        case "headers":
            file_data_type = "Transaction"
        case "lines":
            file_data_type = "Line"
        case "payments":
            file_data_type = "Payment"
        case "cancels":
            file_data_type = "Cancel"
        case _:
            continue

    for j in range(len(outbound_temp_date_folder_list)):
        outbound_temp_date_folder_name: str = outbound_temp_date_folder_list[j].name.replace("/", "")
        outbound_temp_file_list = dbutils.fs.ls(
            azure_storage_path_format.format(os.path.join(jp_ebisumart_outbound_temp_path, outbound_temp_type_folder_name, outbound_temp_date_folder_name))
        )

        for k in range(len(outbound_temp_file_list)):
            outbound_temp_file_name: str = outbound_temp_file_list[k].name.replace("/", "")

            # Copy from temp to actual outbound location.
            if os.path.splitext(outbound_temp_file_name)[-1].lower() == ".csv":
                dbutils.fs.cp(
                    azure_storage_path_format.format(
                        os.path.join(
                            jp_ebisumart_outbound_temp_path,
                            outbound_temp_type_folder_name,
                            outbound_temp_date_folder_name,
                            outbound_temp_file_name
                        )
                    ),
                    azure_storage_path_format.format(
                        os.path.join(
                            jp_ebisumart_outbound_path,
                            outbound_temp_type_folder_name,
                            "BI-RalphLauren_{0}_{1}-{2}.csv".format("JP", file_data_type, data_date.strftime("%Y%m%d"))
                        )
                    )
                )
        del k
    del j
del i

dbutils.fs.rm(
    azure_storage_path_format.format(jp_ebisumart_outbound_temp_path),
    recurse=True
)

print("Completed moving files to Outbound folder")
