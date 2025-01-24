# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for processing Recovery flow of eCom KPI Demand from SharePoint Online for Baozun data (CN/HK)
# MAGIC Data includes
# MAGIC - ~~Demand headers~~
# MAGIC - ~~Demand lines~~
# MAGIC - ~~Demand payments~~
# MAGIC - Cancel lines
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

demand_recovery_cancel_schema: spark_types.StructType = spark_types.StructType([
    spark_types.StructField("order_date", spark_types.StringType(), True),
    spark_types.StructField("order_total", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("store", spark_types.StringType(), True),
    spark_types.StructField("empl_id", spark_types.StringType(), True),
    spark_types.StructField("jda_order", spark_types.StringType(), True),
    spark_types.StructField("line", spark_types.StringType(), True),
    spark_types.StructField("status", spark_types.StringType(), True),
    spark_types.StructField("status_text", spark_types.StringType(), True),
    spark_types.StructField("type", spark_types.StringType(), True),
    spark_types.StructField("transaction_nbr", spark_types.StringType(), True),
    spark_types.StructField("status_date", spark_types.StringType(), True),
    spark_types.StructField("order_demand_amt", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("upc", spark_types.StringType(), True),
    spark_types.StructField("apac_style", spark_types.StringType(), True),
    spark_types.StructField("apac_colour", spark_types.StringType(), True),
    spark_types.StructField("apac_size", spark_types.StringType(), True),
    spark_types.StructField("line_qty", spark_types.DecimalType(38, 0), True),
    spark_types.StructField("line_sub_total", spark_types.DecimalType(38, 2), True),
    spark_types.StructField("disc_promo", spark_types.StringType(), True),
    spark_types.StructField("tender_1", spark_types.StringType(), True),
    spark_types.StructField("tender_2", spark_types.StringType(), True),
    spark_types.StructField("tender_3", spark_types.StringType(), True),
    spark_types.StructField("customer_ip_add", spark_types.StringType(), True),
    spark_types.StructField("order_entry_method", spark_types.StringType(), True),
    spark_types.StructField("csr_associate_name", spark_types.StringType(), True),
    spark_types.StructField("country_code", spark_types.StringType(), True),
    spark_types.StructField("demand_location", spark_types.StringType(), True),
    spark_types.StructField("fulfilling_location", spark_types.StringType(), True),
    spark_types.StructField("original_jda_order", spark_types.StringType(), True),
    spark_types.StructField("warehouse_reason_code", spark_types.StringType(), True),
    spark_types.StructField("warehouse_reason", spark_types.StringType(), True),
    spark_types.StructField("customer_reason_code", spark_types.StringType(), True),
    spark_types.StructField("customer_reason", spark_types.StringType(), True),
    spark_types.StructField("currency", spark_types.StringType(), True),
    spark_types.StructField("webstore", spark_types.StringType(), True)
])

# COMMAND ----------

try:
    demand_recovery_cancel_df: DataFrame = spark.read.format("csv").options(**{
        "sep": ",",
        "encoding": "UTF-8",
        "quote": "\"",
        "escape": "\"",
        "header": True,
        "inferSchema": False,
        "multiLine": False
    }).schema(demand_recovery_cancel_schema).load(azure_storage_demand_recovery_inbound_path_format.format("cancel_recovery_file*.csv"))
except:
    demand_recovery_cancel_df = None
    print("No cancel file found")

# COMMAND ----------

try:
    if(demand_recovery_cancel_df is not None) and (demand_recovery_cancel_df.count() > 0):
        demand_recovery_cancel_result_df: DataFrame = (demand_recovery_cancel_df.alias("cancel")
            .join(baozun_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("cancel.country_code") == spark_functions.col("tender.Country"), spark_functions.col("cancel.tender_1") == spark_functions.col("tender.TenderID")])
            .selectExpr(
                "DATE_FORMAT(TO_TIMESTAMP(cancel.order_date, 'yyyy/MM/dd HH:mm'), 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
                "FORMAT_STRING(cancel.order_total, '%.2f') AS Order_Total",
                "cancel.Store",
                "COALESCE(NULL, 'NULL') AS Empl_Id",
                "-1 AS Jda_Order",
                "cancel.Line",
                "'X' AS Status",
                "'Cancelled' AS Status_Text",
                "COALESCE(NULL, 'NULL') AS Type",
                "cancel.transaction_nbr AS Weborder_Id",
                "DATE_FORMAT(TO_TIMESTAMP(cancel.status_date, 'M/d/yyyy H:m'), 'yyyy-MM-dd HH:mm:ss') AS Status_Date",
                "FORMAT_STRING(cancel.order_demand_amt, '%.2f') AS Order_Demand_Amt",
                "COALESCE(NULL, 'NULL') AS Upc",
                "cancel.APAC_style",
                "cancel.APAC_colour",
                "cancel.APAC_size",
                "FORMAT_STRING(cancel.line_qty, '%.2f') AS Line_Qty",
                "FORMAT_STRING(cancel.line_sub_total, '%.2f') AS Line_Sub_Total",
                "cancel.Disc_Promo",
                "tender.TenderType AS Tender_1",
                "COALESCE(CASE cancel.tender_2 WHEN '(NULL)' THEN 'NULL' ELSE cancel.tender_2 END, 'NULL') AS Tender_2",
                "COALESCE(NULL, 'NULL') AS Tender_3",
                "COALESCE(NULL, 'NULL') AS Customer_Ip_Add",
                "'Website' AS Order_Entry_Method",
                "COALESCE(NULL, 'NULL') AS Csr_Associate_Name",
                "cancel.Country_Code",
                "cancel.Demand_Location",
                "COALESCE(NULL, 'NULL') AS Fulfilling_Location",
                "-1 AS Original_Jda_Order",
                "COALESCE(CASE cancel.warehouse_reason_code WHEN '(NULL)' THEN 'NULL' ELSE cancel.warehouse_reason_code END, 'NULL') AS Warehouse_Reason_Code",
                "COALESCE(CASE cancel.warehouse_reason WHEN '(NULL)' THEN 'NULL' ELSE cancel.warehouse_reason END, 'NULL') AS Warehouse_Reason",
                "COALESCE(CASE cancel.customer_reason_code WHEN '(NULL)' THEN 'NULL' ELSE cancel.customer_reason_code END, 'NULL') AS Customer_Reason_Code",
                "COALESCE(CASE cancel.customer_reason WHEN '(NULL)' THEN 'NULL' ELSE cancel.customer_reason END, 'NULL') AS Customer_Reason",
                "COALESCE(CASE cancel.currency WHEN '(NULL)' THEN 'NULL' ELSE cancel.currency END, 'NULL') AS Currency",
                "cancel.Webstore"
            )
        )
    else:
        demand_recovery_cancel_result_df = None
        print("demand_recovery_cancel_result_df is None")
except:
    demand_recovery_cancel_result_df = None
    print("demand_recovery_cancel_result_df is None due to error")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

if demand_recovery_cancel_result_df is not None:
    demand_recovery_cancel_result_df.coalesce(1).write.format("csv").options(**{
    "sep": "|",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_recovery_outbound_temp_path_format.format(os.path.join("cancels", job_time.strftime("%Y%m%d"))))

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
    if fnmatch.filter([inbound_file_list[i].name], "cancel_*.csv"):
        dbutils.fs.rm(
            azure_storage_demand_recovery_inbound_path_format.format("/".join([inbound_file_list[i].name]))
        )
del i

print("Completed moving files to Outbound folder")
