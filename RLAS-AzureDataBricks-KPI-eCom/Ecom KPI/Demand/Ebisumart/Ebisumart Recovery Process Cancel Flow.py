# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for processing Recovery flow of eCom KPI Demand from Ebisumart (SharePoint Online)
# MAGIC Data includes
# MAGIC - ~~Demand headers~~
# MAGIC - ~~Demand lines~~
# MAGIC - ~~Demand payments~~
# MAGIC - Cancel lines
# MAGIC
# MAGIC External dependencies
# MAGIC - ~~Maven coordinate `com.crealytics:spark-excel_2.12:3.4.1_0.20.3` for reading/writing Excel files (requires to update the version if the Spark version is different)~~
# MAGIC - None

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-level flow
# MAGIC 1. Process file downloaded from SharePoint Online in Azure Storage
# MAGIC 2. Run the ETL
# MAGIC 3. Save the file to Azure Storage Account

# COMMAND ----------

from collections import OrderedDict
import copy
from datetime import date, datetime, timedelta, timezone
import fnmatch
import json
import os
from typing import Dict, List, cast

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_types

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_recovery_path_prefix: str = "EcomKPI/Demand/Recovery"
demand_mapping_path_prefix: str = "EcomKPI/Demand/Mapping"
jp_ebisumart_inbound_path: str = os.path.join(demand_recovery_path_prefix, "Inbound/ebisumart/JP")
# jp_ebisumart_inbound_decoded_path: str = os.path.join(jp_ebisumart_inbound_path, "Decoded")
jp_ebisumart_outbound_temp_path: str = os.path.join(demand_recovery_path_prefix, "Outbound/temp/ebisumart/JP")
jp_ebisumart_outbound_path: str = os.path.join(demand_recovery_path_prefix, "Outbound/ebisumart/JP")
jp_ebisumart_archive_inbound_path: str = os.path.join(demand_recovery_path_prefix, "Archive/Inbound/ebisumart/JP")
jp_ebisumart_archive_outbound_path: str = os.path.join(demand_recovery_path_prefix, "Archive/Outbound/ebisumart/JP")
jp_ebisumart_tender_mapping_path: str = os.path.join(demand_mapping_path_prefix, "EbisumartTenderMapping.csv")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

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

ebisumart_recovery_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("ORDER_DISP_NO", spark_types.StringType(), False),
    spark_types.StructField("ORDER_H_ACCESS_BROWSER_KBN", spark_types.StringType(), False),
    spark_types.StructField("ORDER_DATE", spark_types.StringType(), False),
    spark_types.StructField("KESSAI_ID", spark_types.StringType(), False),
    spark_types.StructField("MEMBER_ID", spark_types.StringType(), True),
    spark_types.StructField("SEIKYU", spark_types.StringType(), False),
    spark_types.StructField("SHIRE_SUM", spark_types.StringType(), False),
    spark_types.StructField("TEIKA_SUM", spark_types.StringType(), False),
    spark_types.StructField("TEIKA_SUM_TAX", spark_types.StringType(), False),
    spark_types.StructField("TAX", spark_types.StringType(), False),
    spark_types.StructField("SORYO", spark_types.StringType(), False),
    spark_types.StructField("SORYO_TAX", spark_types.StringType(), False),
    spark_types.StructField("DAIBIKI", spark_types.StringType(), False),
    spark_types.StructField("DAIBIKI_TAX", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_FREE_ITEM31", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_NO", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_FREE_ITEM41", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_FREE_ITEM32", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_TEIKA", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_SHIRE_PRICE", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_QUANTITY", spark_types.StringType(), False),
    spark_types.StructField("ORDER_D_FREE_ITEM35", spark_types.StringType(), True),
    spark_types.StructField("FREE_ITEM33", spark_types.StringType(), True),
    spark_types.StructField("COUPON_WARIBIKI", spark_types.StringType(), True),
    spark_types.StructField("COUPON_ID", spark_types.StringType(), True),
    spark_types.StructField("SHIP_SLIP_NO", spark_types.StringType(), True),
    spark_types.StructField("CANCEL_DATE", spark_types.StringType(), True),
    spark_types.StructField("SEND_DATE", spark_types.StringType(), True)
])

ebisumart_recovery_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(ebisumart_recovery_schema).load(azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_path, "JP_cancellation*.csv")))

# COMMAND ----------

ebisumart_recovery_renamed_df: DataFrame = (ebisumart_recovery_df
                        .withColumn(
                            "Item_Type",
                            spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                                .otherwise(spark_functions.col("ORDER_D_FREE_ITEM31"))
                        )
                        .withColumn(
                            "eCom_Store_ID",
                            spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.lit("57037"))
                                .when(spark_functions.col("Item_Type") == spark_functions.lit("L"), spark_functions.lit("56001"))
                                .otherwise(spark_functions.lit("57036"))
                        )
                        .withColumn("Transaction_Nbr", spark_functions.regexp_replace("ORDER_DISP_NO", spark_functions.lit(r"^(?:ORDER|e)"), spark_functions.lit("")))
                        .withColumn("Order_Date",
                            spark_functions.date_format(
                                spark_functions.to_timestamp(spark_functions.col("ORDER_DATE"), "M/d/yyyy H:m"),
                                "yyyy-MM-dd HH:mm:ss"
                            )
                        )
                        .withColumn("Cancel_Date",
                            spark_functions.date_format(
                                spark_functions.to_timestamp(spark_functions.col("CANCEL_DATE"), "M/d/yyyy"),
                                "yyyy-MM-dd HH:mm:ss"
                            )
                        )
                        .withColumn("Country", spark_functions.lit("JP"))
                        .withColumn("Device_Type_ID", spark_functions.left(spark_functions.col("ORDER_H_ACCESS_BROWSER_KBN"), spark_functions.lit(1)).cast(spark_types.IntegerType()))
                        .withColumn("Device_Type",
                            spark_functions.when(spark_functions.col("Device_Type_ID").between(1, 4),
                                                 spark_functions.element_at(spark_functions.lit(["DESKTOP", "MOBILE", "MOBILE PHONE", "TABLET"]), spark_functions.col("Device_Type_ID")))
                            .otherwise(spark_functions.lit("OTHER"))
                        )
                        .withColumn("Employee_Purchase", spark_functions.lit(None).cast(spark_types.StringType()))
                        .withColumn("Order_Channel", spark_functions.lit("Website"))
                        .withColumn("Customer_ID",
                            spark_functions.when(spark_functions.col("MEMBER_ID") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                            .otherwise(spark_functions.col("MEMBER_ID")))
                        .withColumn("Shipment_ID",
                            spark_functions.when(spark_functions.col("SHIP_SLIP_NO") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                            .otherwise(spark_functions.col("SHIP_SLIP_NO")))
                        .withColumn("Shipment_Method", spark_functions.lit(None).cast(spark_types.StringType()))
                        .withColumn("Ship_To_Country", spark_functions.lit("JP"))
                        .withColumn("Ship_To", spark_functions.lit(None).cast(spark_types.StringType()))
                        .withColumn("Bill_To", spark_functions.lit(None).cast(spark_types.StringType()))
                        .withColumn("Promotion_Coupons",
                            spark_functions.when(spark_functions.coalesce(spark_functions.col("COUPON_ID"), spark_functions.lit("")) == spark_functions.lit(""), spark_functions.lit("N"))
                            .otherwise(spark_functions.lit("Y"))
                        )
                        .withColumn("Freight_Tax", spark_functions.col("SORYO_TAX").cast(spark_types.DoubleType()))
                        .withColumn("Demand_Gross", spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.col("TEIKA_SUM_TAX").cast(spark_types.DoubleType()))
                        .withColumn("Cancel_Whole_Order_Demand_Total", spark_functions.sum(spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.col("TEIKA_SUM_TAX").cast(spark_types.DoubleType())).over(Window.partitionBy("Transaction_Nbr")))
                        # .withColumn("Demand_Net", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()) - spark_functions.col("TAX").cast(spark_types.DoubleType()) - spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) + spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) - spark_functions.col("FREE_ITEM11").cast(spark_types.DoubleType()))
                        .withColumn("Demand_Net", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()) - spark_functions.col("TAX").cast(spark_types.DoubleType()) - spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()) + spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()))
                        .withColumn("Employee_ID", spark_functions.lit(None).cast(spark_types.StringType()))
                        .withColumn("Bill_To_Country", spark_functions.lit("JP"))
                        .withColumn("Demand_Currency", spark_functions.lit("JPY"))
                        .withColumn("Freight_Amount", spark_functions.col("SORYO").cast(spark_types.DoubleType()))
                        .withColumn("Discount_Amount",
                            spark_functions.col("SHIRE_SUM").cast(spark_types.DoubleType()) - spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31") == spark_functions.lit("C"), spark_functions.col("ORDER_D_FREE_ITEM35").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType()))
                        )
                        .withColumn("Cancel_Discount_Amount",
                            spark_functions.col("SHIRE_SUM").cast(spark_types.DoubleType()) + spark_functions.col("FREE_ITEM33").cast(spark_types.DoubleType()) - spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31") == spark_functions.lit("C"), spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType()))
                        )
                        .withColumn("Coupon_Amount", spark_functions.lit("COUPON_WARIBIKI").cast(spark_types.DoubleType()))
                        .withColumn("Tax", spark_functions.col("TAX").cast(spark_types.DoubleType()))
                        .withColumn("CoD_Charge", spark_functions.col("DAIBIKI").cast(spark_types.DoubleType()))
                        .withColumn("CoD_Charge_Tax", spark_functions.col("DAIBIKI_TAX").cast(spark_types.DoubleType()))
                        .withColumn("Shipping_Fee", spark_functions.col("SORYO").cast(spark_types.DoubleType()))
                        .withColumn("Shipping_Fee_Tax", spark_functions.col("SORYO_TAX").cast(spark_types.DoubleType()))
                        .withColumn("Line_Total", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()))
                        .withColumn("Tender_ID", spark_functions.col("KESSAI_ID"))
                        .withColumn("Tender_Amt", spark_functions.col("SEIKYU").cast(spark_types.DoubleType()))
                        # .withColumn("Gift_Charge", spark_functions.col("FREE_ITEM11").cast(spark_types.DoubleType()))
                        .withColumn("Gift_Charge", spark_functions.lit(None).cast(spark_types.DoubleType()))
                        .withColumn("Gift_Tax", spark_functions.col("Gift_Charge") * spark_functions.lit(0.1))
                        .withColumn("Jda_Order", spark_functions.lit(-1))
)

header_df: DataFrame = ebisumart_recovery_renamed_df.select("eCom_Store_ID", "Transaction_Nbr", "Order_Date", "Cancel_Date", "Country", "Device_Type", "Employee_Purchase", "Order_Channel", "Customer_ID", "Shipment_ID", "Shipment_Method", "Ship_To_Country", "Ship_To", "Bill_To", "Promotion_Coupons", "Freight_Tax", "Demand_Gross", "Cancel_Whole_Order_Demand_Total", "Demand_Net", "Employee_ID", "Bill_To_Country", "Demand_Currency", "Freight_Amount", "Discount_Amount", "Coupon_Amount", "Tax", "CoD_Charge", "CoD_Charge_Tax", "Shipping_Fee", "Shipping_Fee_Tax", "Line_Total", "Tender_ID", "Tender_Amt", "Item_Type", "Gift_Charge", "Gift_Tax", "Jda_Order")

# COMMAND ----------

line_df: DataFrame = (ebisumart_recovery_df
                      .withColumn(
                          "Item_Type",
                          spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM31") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                              .otherwise(spark_functions.col("ORDER_D_FREE_ITEM31"))
                      )
                      .withColumn(
                          "eCom_Store_ID",
                          spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.lit("57037"))
                              .when(spark_functions.col("Item_Type") == spark_functions.lit("L"), spark_functions.lit("56001"))
                              .otherwise(spark_functions.lit("57036"))
                      )
                      .withColumn("Transaction_Nbr", spark_functions.regexp_replace("ORDER_DISP_NO", spark_functions.lit(r"^(?:ORDER|e)"), spark_functions.lit("")))
                      .withColumn("Order_Date",
                          spark_functions.date_format(
                              spark_functions.to_timestamp(spark_functions.col("ORDER_DATE"), "M/d/yyyy H:m"),
                              "yyyy-MM-dd HH:mm:ss"
                          )
                      )
                      .withColumn("Cancel_Date",
                          spark_functions.date_format(
                              spark_functions.to_timestamp(spark_functions.col("CANCEL_DATE"), "M/d/yyyy"),
                              "yyyy-MM-dd HH:mm:ss"
                          )
                      )
                      .withColumn("Shipment_ID",
                          spark_functions.when(spark_functions.col("SHIP_SLIP_NO") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                          .otherwise(spark_functions.col("SHIP_SLIP_NO")))
                      .withColumn("Shipment_Method", spark_functions.lit(None).cast(spark_types.StringType()))
                      .withColumn("Ship_To_Country", spark_functions.lit("JP"))
                      .withColumn("Item_Seq_Number", spark_functions.col("ORDER_D_NO").cast(spark_types.IntegerType()))
                      .withColumn("Barcode",
                                  spark_functions.when(spark_functions.col("ORDER_D_FREE_ITEM32") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType()))
                                  .otherwise(spark_functions.col("ORDER_D_FREE_ITEM32")))
                      .withColumn("Demand_Unit_Price", spark_functions.col("ORDER_D_TEIKA").cast(spark_types.DoubleType()))
                      .withColumn("Demand_Sales_Unit", spark_functions.col("ORDER_D_QUANTITY").cast(spark_types.IntegerType()))
                      .withColumn("Demand_Net", spark_functions.col("ORDER_D_SHIRE_PRICE") * spark_functions.lit(1.1))
                      .withColumn("Cancel_Line_Total", (spark_functions.col("TEIKA_SUM").cast(spark_types.DoubleType()) + spark_functions.col("TEIKA_SUM_TAX").cast(spark_types.DoubleType())) * spark_functions.col("Demand_Sales_Unit"))
                      .withColumn("Discount_Amount", (spark_functions.col("ORDER_D_SHIRE_PRICE").cast(spark_types.DoubleType()) + spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.col("ORDER_D_FREE_ITEM35").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType())) - spark_functions.col("Demand_Unit_Price")) * spark_functions.col("Demand_Sales_Unit"))
                      .withColumn("Cancel_Discount_Amount", (spark_functions.col("SHIRE_SUM").cast(spark_types.DoubleType()) + spark_functions.when(spark_functions.col("Item_Type") == spark_functions.lit("C"), spark_functions.col("FREE_ITEM33").cast(spark_types.DoubleType())).otherwise(spark_functions.lit(0).cast(spark_types.DoubleType())) - spark_functions.col("TEIKA_SUM")))
                      .withColumn("Gift_IND", spark_functions.lit(None).cast(spark_types.StringType()))
                      .withColumn("Gift_Charge", spark_functions.lit(None).cast(spark_types.DoubleType()))
                      .withColumn("Gift_Tax", spark_functions.col("Gift_Charge") * spark_functions.lit(0.1))
                      .withColumn("Non_Merch_Item", spark_functions.lit("N"))
                      .withColumn("Cancelled_Status", spark_functions.lit("X"))
                      .withColumn("Cancelled_Status_Text", spark_functions.lit("Cancelled"))
                      .withColumn("Cancelled_Type", spark_functions.lit(None).cast(spark_types.StringType()))
)

# COMMAND ----------

demand_recovery_cancel_header_df: DataFrame = ebisumart_recovery_renamed_df.filter("CANCEL_DATE IS NOT NULL")
data_date: datetime = datetime.strptime(
    header_df.selectExpr("MIN(CANCEL_DATE) AS date").collect()[0]["date"],
    "%Y-%m-%d %H:%M:%S"
)
demand_recovery_cancel_header_final_df: DataFrame = (demand_recovery_cancel_header_df.alias("header")
    .join(line_df.filter("CANCEL_DATE IS NOT NULL").alias("line"), how="left", on=[spark_functions.col("header.eCom_Store_ID") == spark_functions.col("line.eCom_Store_ID"), spark_functions.col("header.Transaction_Nbr") == spark_functions.col("line.Transaction_Nbr"), spark_functions.col("header.ORDER_D_NO") == spark_functions.col("line.Item_Seq_Number")])
    .join(item_mapping_df.alias("item"), how="left", on=[spark_functions.col("line.Barcode") == spark_functions.col("item.Barcode")])
    # .join(ebisumart_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("header.Tender_ID") == spark_functions.col("tender.TenderID")])
).selectExpr(
    "header.Order_Date",
    "FORMAT_STRING('%.2f', header.Line_Total) AS Order_Total",
    "header.eCom_Store_ID AS Store",
    # "Employee_ID AS Empl_Id",
    "COALESCE(NULL, 'NULL') AS Empl_Id",
    "Jda_Order",
    "Item_Seq_Number AS Line",
    "Cancelled_Status AS Status",
    "Cancelled_Status_Text AS Status_Text",
    # "Cancelled_Type AS Type",
    "COALESCE(NULL, 'NULL') AS Type",
    "header.Transaction_Nbr AS Weborder_Id",
    "header.Cancel_Date AS Status_Date",
    "FORMAT_STRING('%.2f', header.Cancel_Whole_Order_Demand_Total) AS Order_Demand_Amt",
    "COALESCE(line.Barcode, 'NULL') AS Upc",
    "COALESCE(TRIM(item.Style), 'NULL') AS APAC_style",
    "COALESCE(TRIM(item.Color), 'NULL') AS APAC_colour",
    "COALESCE(TRIM(item.Size), 'NULL') AS APAC_size",
    "FORMAT_STRING('%.2f', CAST(line.Demand_Sales_Unit AS DOUBLE)) AS Line_Qty",
    "FORMAT_STRING('%.2f', line.Cancel_Line_Total) AS Line_Sub_Total",
    "FORMAT_STRING('%.2f', line.Cancel_Discount_Amount) AS Disc_Promo",
    # "tender.TenderType AS Tender_1",
    "header.Tender_ID AS Tender_1",
    "COALESCE(NULL, 'NULL') AS Tender_2",
    "COALESCE(NULL, 'NULL') AS Tender_3",
    "COALESCE(NULL, 'NULL') AS Customer_Ip_Add",
    "'Website' AS Order_Entry_Method",
    "COALESCE(NULL, 'NULL') AS Csr_Associate_Name",
    "Country AS Country_Code",
    "header.eCom_Store_ID AS Demand_Location",
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
    demand_recovery_cancel_header_final_df
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

inbound_file_list = dbutils.fs.ls(
    azure_storage_path_format.format(jp_ebisumart_inbound_path)
)

for i in range(len(inbound_file_list)):
    # Delete all CSV file in non-archive inbound path.
    if fnmatch.filter([inbound_file_list[i].name], "*.csv"):
        dbutils.fs.rm(
            azure_storage_path_format.format("/".join([jp_ebisumart_inbound_path, inbound_file_list[i].name]))
        )

del i

# COMMAND ----------

outbound_temp_type_folder_list = dbutils.fs.ls(azure_storage_path_format.format(jp_ebisumart_outbound_temp_path))

for i in range(len(outbound_temp_type_folder_list)):
    outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
    outbound_temp_date_folder_list = dbutils.fs.ls(
        azure_storage_path_format.format(os.path.join(jp_ebisumart_outbound_temp_path, outbound_temp_type_folder_name))
    )

    file_data_type: str
    match outbound_temp_type_folder_name:
        # case "headers":
        #     file_data_type = "Transaction"
        # case "lines":
        #     file_data_type = "Line"
        # case "payments":
        #     file_data_type = "Payment"
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
