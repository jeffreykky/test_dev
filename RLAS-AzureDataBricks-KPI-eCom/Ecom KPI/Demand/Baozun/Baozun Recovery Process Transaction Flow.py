# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for processing Recovery flow of eCom KPI Demand from SharePoint Online for Baozun data (CN/HK)
# MAGIC Data includes
# MAGIC - Demand headers
# MAGIC - Demand lines
# MAGIC - Demand payments
# MAGIC - ~~Cancel lines~~
# MAGIC
# MAGIC External dependencies
# MAGIC - NIL

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-level flow
# MAGIC 1. Process file in input staging folder Azure Storage
# MAGIC 1. Save processed data as CSV file in output staging folder
# MAGIC 1. Delete files in input staging folder in Azure Storage

# COMMAND ----------

from collections import OrderedDict
import copy
from datetime import date, datetime, timedelta, timezone
import fnmatch
import json
import os
from typing import Dict, List, cast

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_recovery_path_prefix: str = "EcomKPI/Demand/Recovery"
demand_mapping_path_prefix: str = "EcomKPI/Demand/Mapping"

demand_recovery_inbound_path_format: str = os.path.join(demand_recovery_path_prefix, "Inbound/baozun/{0}")
demand_recovery_archive_inbound_path_format: str = os.path.join(demand_recovery_path_prefix, "Archive/Inbound/baozun/{0}")
demand_recovery_outbound_temp_suffix_format: str = os.path.join(demand_recovery_path_prefix, "Outbound/temp/baozun/{0}")
demand_recovery_outbound_suffix_format: str = os.path.join(demand_recovery_path_prefix, "Outbound/baozun/{0}")
demand_recovery_archive_outbound_suffix_format: str = os.path.join(demand_recovery_path_prefix, "Archive/Outbound/baozun/{0}")

# demand_recovery_archive_cn_inbound_suffix_format: str = demand_recovery_archive_inbound_path_format.format("CN/{0}")
# demand_recovery_archive_hk_inbound_suffix_format: str = demand_recovery_archive_inbound_path_format.format("HK/{0}")

cn_baozun_tender_mapping_path: str = os.path.join(demand_mapping_path_prefix, "BaozunTenderMapping.csv")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)
print(azure_storage_path_format)

# COMMAND ----------

azure_storage_demand_recovery_inbound_path_format: str = azure_storage_path_format.format(demand_recovery_inbound_path_format)
azure_storage_demand_recovery_outbound_temp_path_format: str = azure_storage_path_format.format(demand_recovery_outbound_temp_suffix_format)
azure_storage_demand_recovery_outbound_path_format: str = azure_storage_path_format.format(demand_recovery_outbound_suffix_format)

azure_storage_demand_recovery_archive_outbound_path_format: str = azure_storage_path_format.format(demand_recovery_archive_outbound_suffix_format)
print(azure_storage_demand_recovery_archive_outbound_path_format)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_types

# COMMAND ----------

baozun_tender_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("Country", spark_types.StringType(), False),
    spark_types.StructField("TenderID", spark_types.StringType(), False),
    spark_types.StructField("TenderType", spark_types.StringType(), False)
])

baozun_tender_mapping_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(baozun_tender_schema).load(azure_storage_path_format.format(cn_baozun_tender_mapping_path))
display(baozun_tender_mapping_df)

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

demand_recovery_transaction_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("ecom_store_id", spark_types.StringType(), True),
    spark_types.StructField("transaction_nbr", spark_types.StringType(), True),
    spark_types.StructField("order_date", spark_types.StringType(), True),
    spark_types.StructField("country", spark_types.StringType(), True),
    spark_types.StructField("device_type", spark_types.IntegerType(), True),
    spark_types.StructField("employee_purchase", spark_types.StringType(), True),
    spark_types.StructField("order_channel", spark_types.StringType(), True),
    spark_types.StructField("customer_id", spark_types.StringType(), True),
    spark_types.StructField("ship_to", spark_types.StringType(), True),
    spark_types.StructField("bill_to", spark_types.StringType(), True),
    spark_types.StructField("promotion_coupons", spark_types.StringType(), True),
    spark_types.StructField("freight_tax", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("demand_net", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("employee_id", spark_types.StringType(), True),
    spark_types.StructField("bill_to_country", spark_types.StringType(), True),
    spark_types.StructField("freight_amount", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("discount_amount", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("coupon_amount", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("tax", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("cod_charge", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("cod_charge_tax", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("shipping_fee", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("shipping_fee_tax", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("line_total", spark_types.DecimalType(38, 2), True),
])

demand_recovery_line_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("ecom_store_id", spark_types.StringType(), True),
    spark_types.StructField("transaction_nbr", spark_types.StringType(), True),
    spark_types.StructField("order_date", spark_types.StringType(), True),
    spark_types.StructField("ax_style_id", spark_types.StringType(), True),
    spark_types.StructField("ax_color_id", spark_types.StringType(), True),
    spark_types.StructField("ax_size_id", spark_types.StringType(), True),
    spark_types.StructField("demand_net", spark_types.StringType(), True),
    spark_types.StructField("demand_sales_unit", spark_types.StringType(), True),
    spark_types.StructField("shipment_id", spark_types.StringType(), True),
    spark_types.StructField("shipment_method", spark_types.StringType(), True),
    spark_types.StructField("ship_to_country", spark_types.StringType(), True),
    spark_types.StructField("gift_ind", spark_types.StringType(), True),
    spark_types.StructField("non_merch_item", spark_types.StringType(), True),
    spark_types.StructField("gift_charge", spark_types.StringType(), True),
    spark_types.StructField("gift_tax", spark_types.StringType(), True),
    spark_types.StructField("item_seq_number", spark_types.StringType(), True),
    spark_types.StructField("discount_amount", spark_types.DecimalType(38, 2), True)
])

demand_recovery_payment_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("ecom_store_id", spark_types.StringType(), True),
    spark_types.StructField("transaction_nbr", spark_types.StringType(), True),
    spark_types.StructField("order_date", spark_types.StringType(), True),
    spark_types.StructField("tender", spark_types.StringType(), True),
    spark_types.StructField("tender_amt", spark_types.StringType(), True),
    spark_types.StructField("demand_currency", spark_types.StringType(), True)
])

# COMMAND ----------

try:
    demand_recovery_transaction_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(demand_recovery_transaction_schema).load(azure_storage_demand_recovery_inbound_path_format.format("demand_transaction_recovery_file*.csv"))
except:
    demand_recovery_transaction_df = None
    print("No transaction file found")

try:
    demand_recovery_line_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(demand_recovery_line_schema).load(azure_storage_demand_recovery_inbound_path_format.format("demand_line_recovery_file*.csv"))
except:
    demand_recovery_line_df = None
    print("No Line file found")

try:
    demand_recovery_payment_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "\"",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(demand_recovery_payment_schema).load(azure_storage_demand_recovery_inbound_path_format.format("demand_payment_recovery_file*.csv"))
except:
    demand_recovery_payment_df = None
    print("No payment file found")

# COMMAND ----------

try:
    if(demand_recovery_transaction_df is not None) and (demand_recovery_transaction_df.count() > 0):
        demand_recovery_transaction_result_df: DataFrame = (demand_recovery_transaction_df
            .withColumn("Device_Type", spark_functions.when(
                spark_functions.col("device_type").between(1, 2),
                spark_functions.element_at(spark_functions.lit(["Desktop", "Mobile Phone"]), spark_functions.col("device_type"))
            ))
            .selectExpr(
                "ecom_store_id AS eCom_Store_ID",
                "transaction_nbr AS Transaction_Nbr",
                #"order_date AS Order_Date",
                "date_format(to_timestamp(order_date, 'yyyy/MM/dd HH:mm'), 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
                "country AS Country",
                "Device_Type",
                "employee_purchase AS Employee_Purchase",
                "'Website' AS Order_Channel",
                "-1 AS Customer_ID",
                "ship_to AS Ship_To",
                "bill_to AS Bill_To",
                "COALESCE(NULL, 'NULL') AS Promotion_Coupons",
                "FORMAT_STRING(freight_tax, '%.2f') AS Freight_Tax",
                "FORMAT_STRING(demand_net, '%.2f') AS Demand_Net",
                "COALESCE(NULL, 'NULL') AS Employee_ID",
                "bill_to_country AS Bill_To_Country",
                "FORMAT_STRING(freight_amount, '%.2f') AS Freight_Amount",
                "FORMAT_STRING(discount_amount, '%.2f') AS Discount_Amount",
                "FORMAT_STRING(coupon_amount, '%.2f') AS Coupon_Amount",
                "FORMAT_STRING(tax, '%.2f') AS Tax"
            )
        )
    else:
        demand_recovery_transaction_result_df = None
        print("demand_recovery_transaction_result_df is None")
except:
    demand_recovery_transaction_result_df = None
    print("demand_recovery_transaction_result_df is None due to error")

# COMMAND ----------

try:
    if(demand_recovery_line_df is not None) and (demand_recovery_line_df.count() > 0):
        demand_recovery_line_result_df: DataFrame = (demand_recovery_line_df
            .selectExpr(
                "ecom_store_id AS eCom_Store_ID",
                "transaction_nbr AS Transaction_Nbr",
                #"order_date AS Order_Date",
                "date_format(to_timestamp(order_date, 'yyyy/MM/dd HH:mm'), 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
                "ax_style_id AS AX_Style_ID",
                "ax_color_id AS AX_Color_ID",
                "ax_size_id AS AX_Size_ID",
                "demand_net AS Demand_Net",
                "demand_sales_unit AS Demand_Sales_Unit",
                "'0' AS Shipment_ID",
                "shipment_method AS Shipment_Method",
                "ship_to_country AS Ship_To_Country",
                "'0' AS Gift_IND",
                "'N' AS Non_Merch_Item",
                "'0' AS GiftChrg",
                "'0' AS GiftTax",
                "item_seq_number AS ItmSeqNum",
                "FORMAT_STRING(discount_amount, '%.2f') AS DiscountAmt"
            )
        )
    else:
        demand_recovery_line_result_df = None
        print("demand_recovery_line_result_df is None")
except:
    demand_recovery_line_result_df = None
    print("demand_recovery_line_result_df is None due to error")

# COMMAND ----------

try:
    if(demand_recovery_payment_df is not None) and (demand_recovery_payment_df.count() > 0):
        demand_recovery_payment_result_df: DataFrame = (demand_recovery_payment_df
            .withColumn("Country",
                spark_functions.when(spark_functions.col("ecom_store_id").isin(["07301", "07302", "07304"]), spark_functions.lit("CN")).when(spark_functions.col("ecom_store_id").isin(["07308"]), spark_functions.lit("HK")).otherwise(spark_functions.lit(None))
            )
            .alias("payment")
            .join(baozun_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("payment.Country") == spark_functions.col("tender.Country"), spark_functions.col("payment.Tender") == spark_functions.col("tender.TenderID")])
            .selectExpr(
                "payment.eCom_Store_ID",
                "payment.Transaction_Nbr",
                #"payment.Order_Date",
                "date_format(to_timestamp(payment.Order_Date, 'yyyy/MM/dd HH:mm'), 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
                "tender.TenderType AS Tender",
                "FORMAT_STRING(CAST((CASE WHEN COALESCE(payment.tender_amt, '') = '' THEN 0 ELSE payment.tender_amt END) AS DECIMAL(38, 2)), '%.2f') AS Tender_Amt",
                "payment.Demand_Currency"
            )
        )
    else:
        demand_recovery_payment_result_df = None
        print("demand_recovery_payment_result_df is None")
except:
    demand_recovery_payment_result_df = None
    print("demand_recovery_payment_result_df is None due to error")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

if demand_recovery_transaction_result_df is not None:
    demand_recovery_transaction_result_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join("headers", job_time.strftime("%Y%m%d"))))

if demand_recovery_line_result_df is not None:
    demand_recovery_line_result_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join("lines", job_time.strftime("%Y%m%d"))))

if demand_recovery_payment_result_df is not None:
    demand_recovery_payment_result_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join("payments", job_time.strftime("%Y%m%d"))))

# COMMAND ----------

try:
    outbound_temp_type_folder_list = dbutils.fs.ls(
        azure_storage_demand_recovery_outbound_temp_path_format.format("")
    )

    for i in range(len(outbound_temp_type_folder_list)):
        outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
        outbound_temp_date_folder_list = dbutils.fs.ls(
            azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name))
        )

        file_data_type: str
        match outbound_temp_type_folder_name:
            case "headers":
                file_data_type = "Transaction"
            case "lines":
                file_data_type = "Line"
            case "payments":
                file_data_type = "Payment"
            # case "cancels":
            #     file_data_type = "Cancel"
            case _:
                continue

        for j in range(len(outbound_temp_date_folder_list)):
            outbound_temp_date_folder_name: str = outbound_temp_date_folder_list[j].name.replace("/", "")
            outbound_temp_file_list = dbutils.fs.ls(
                azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name, outbound_temp_date_folder_name))
            )

            for k in range(len(outbound_temp_file_list)):
                outbound_temp_file_name: str = outbound_temp_file_list[k].name.replace("/", "")

                # Copy from temp to actual outbound location.
                if fnmatch.filter([outbound_temp_file_name], "*.csv"):
                    dbutils.fs.cp(
                        azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name, outbound_temp_date_folder_name, outbound_temp_file_name)),
                        azure_storage_demand_recovery_outbound_path_format.format(os.path.join(outbound_temp_type_folder_name, "BI-RalphLauren_CN_HK_{0}-{1}.csv".format(file_data_type, outbound_temp_date_folder_name)))
                    )
            del k
        del j
    del i
except Exception as e:
    print(e)
dbutils.fs.rm(
    azure_storage_demand_recovery_outbound_temp_path_format.format(""),
    recurse=True
)

#delete inbound staging
inbound_file_list = dbutils.fs.ls(
    azure_storage_demand_recovery_inbound_path_format.format(""))
for i in range(len(inbound_file_list)):
    # Delete all JSON file in non-archive inbound path.
    if fnmatch.filter([inbound_file_list[i].name], "demand_*.csv"):
        dbutils.fs.rm(
            azure_storage_demand_recovery_inbound_path_format.format("/".join([inbound_file_list[i].name]))
        )
del i

print("Completed moving files to Outbound folder")
