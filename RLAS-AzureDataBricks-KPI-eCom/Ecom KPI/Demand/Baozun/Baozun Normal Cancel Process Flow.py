# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to process Baozun Normal Cancel Flow
# MAGIC External dependencies
# MAGIC - PIP: requests (already pre-installed in Databricks Runtime)

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

# Default value for normal flow
now: datetime = datetime.now(timezone(timedelta(hours=8)))

# Or, choose a fixed datetime
# year: int = 2024
# month: int = 5
# day: int = 1
# now: datetime = datetime(year, month, day, tzinfo=timezone(timedelta(hours=8)))

to_order_date: date = now.date()
from_order_date: date = to_order_date + timedelta(days=-1)

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_normal_path_prefix: str = "EcomKPI/Demand/Normal"
demand_mapping_path_prefix: str = "EcomKPI/Demand/Mapping"
demand_path_inbound_suffix_format: str = os.path.join(demand_normal_path_prefix, "Inbound/baozun/{0}")
demand_path_outbound_temp_suffix_format: str = os.path.join(demand_normal_path_prefix, "Outbound/temp/baozun/{0}")
demand_path_outbound_suffix_format: str = os.path.join(demand_normal_path_prefix, "Outbound/baozun/{0}")
demand_path_archive_outbound_suffix_format: str = os.path.join(demand_normal_path_prefix, "Archive/Outbound/baozun/{0}")
cn_baozun_tender_mapping_path: str = os.path.join(demand_mapping_path_prefix, "BaozunTenderMapping.csv")
cn_baozun_cancel_json_schema_path: str = os.path.join(demand_mapping_path_prefix, "BaozunCancelJsonSchema.json")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_demand_inbound_path_format: str = azure_storage_path_format.format(demand_path_inbound_suffix_format)
azure_storage_demand_outbound_temp_path_format: str = azure_storage_path_format.format(demand_path_outbound_temp_suffix_format)
azure_storage_demand_outbound_path_format: str = azure_storage_path_format.format(demand_path_outbound_suffix_format)

azure_storage_demand_archive_outbound_path_format: str = azure_storage_path_format.format(demand_path_archive_outbound_suffix_format)

azure_storage_baozun_cancel_json_schema_path: str = azure_storage_path_format.format(cn_baozun_cancel_json_schema_path)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_types

# COMMAND ----------

temp_cn_baozun_cancel_json_schema_path: str = os.path.join(".", cn_baozun_cancel_json_schema_path)
dbutils.fs.cp(azure_storage_baozun_cancel_json_schema_path, temp_cn_baozun_cancel_json_schema_path)

# json_schema_dict: Dict[str, object] = json.loads("""
# {"fields":[{"metadata":{},"name":"code","nullable":true,"type":"long"},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"orders","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"pageSize","nullable":true,"type":"long"},{"metadata":{},"name":"pages","nullable":true,"type":"long"},{"metadata":{},"name":"success","nullable":true,"type":"boolean"},{"metadata":{},"name":"total","nullable":true,"type":"long"}],"type":"struct"}
# """)

json_schema_dict: Dict[str, object]
with open("/dbfs/{}".format(temp_cn_baozun_cancel_json_schema_path), "r") as f:
    json_schema_dict = cast(Dict[str, object], json.load(f))

json_schema = spark_types.StructType.fromJson(json_schema_dict)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC dfs: Dict[str, DataFrame] = {}
# MAGIC orders_dfs: Dict[str, DataFrame] = {}
# MAGIC order_cancels_dfs: Dict[str, DataFrame] = {}
# MAGIC
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     shop_json_path: str = azure_storage_demand_inbound_path_format.format(os.path.join(region, "*-Cancel-*.json"))
# MAGIC     dfs[region]: DataFrame = (
# MAGIC         spark.read.format("json")
# MAGIC         .schema(json_schema)
# MAGIC         .load(shop_json_path)
# MAGIC         .withColumn("__fileName", spark_functions.input_file_name())
# MAGIC     )
# MAGIC     orders_dfs[region]: DataFrame = (dfs[region].withColumn("orders", spark_functions.explode("orders"))
# MAGIC                                      .select("orders.*", "__fileName")
# MAGIC                                      .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
# MAGIC                                      .withColumn("Order_Total", spark_functions.col("Order_Total").cast(spark_types.DoubleType()))
# MAGIC                                      .withColumn("Empl_Id", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Jda_Order", spark_functions.lit(-1).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Status", spark_functions.lit("X"))
# MAGIC                                      .withColumn("Status_Text", spark_functions.lit("Cancelled"))
# MAGIC                                      # .withColumn("Type", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Status_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Status_Date")))
# MAGIC                                      .withColumn("Order_Demand_Amt", spark_functions.col("Order_Demand_Amt").cast(spark_types.DoubleType()))
# MAGIC                                      # .withColumn("Upc", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Line_Qty", spark_functions.col("Line_Qty").cast(spark_types.IntegerType()))
# MAGIC                                      .withColumn("Line_Sub_Total", spark_functions.col("Line_Sub_Total").cast(spark_types.DoubleType()))
# MAGIC                                      # .withColumn("Tender_3", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Customer_Ip_Add", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Order_Entry_Method", spark_functions.lit("Website"))
# MAGIC                                      .withColumn("Csr_Associate_Name", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                      .withColumn("Fulfilling_Location", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC                                     )
# MAGIC
# MAGIC     order_cancels_dfs[region]: DataFrame = (
# MAGIC         orders_dfs[region]
# MAGIC         .alias("cancel")
# MAGIC         .join(
# MAGIC             baozun_tender_mapping_df.alias("tender"),
# MAGIC             how="left",
# MAGIC             on=[spark_functions.col("cancel.Country_Code") == spark_functions.col("tender.Country"), spark_functions.col("cancel.Tender_1") == spark_functions.col("tender.TenderID")]
# MAGIC         )
# MAGIC         .selectExpr(
# MAGIC             "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
# MAGIC             "ROUND(Order_Total, '0.00') AS Order_Total",
# MAGIC             "Store",
# MAGIC             "Empl_Id",
# MAGIC             "Jda_Order",
# MAGIC             "Line",
# MAGIC             "Status",
# MAGIC             "Status_Text",
# MAGIC             "Type",
# MAGIC             "Transaction_Nbr AS Weborder_Id",
# MAGIC             "DATE_FORMAT(Status_Date, 'yyyy-MM-dd HH:mm:ss') AS Status_Date",
# MAGIC             "ROUND(Order_Demand_Amt, '0.00') AS Order_Demand_Amt",
# MAGIC             "Upc",
# MAGIC             "APAC_style",
# MAGIC             "APAC_colour",
# MAGIC             "APAC_size",
# MAGIC             "ROUND(Line_Qty, '0.00') AS Line_Qty",
# MAGIC             "ROUND(Line_Sub_Total, '0.00') AS Line_Sub_Total",
# MAGIC             "Disc_Promo",
# MAGIC             "COALESCE(tender.TenderType, 'OTHER') AS Tender_1",
# MAGIC             "Tender_2",
# MAGIC             "Tender_3",
# MAGIC             "Customer_Ip_Add",
# MAGIC             "Order_Entry_Method",
# MAGIC             "Csr_Associate_Name",
# MAGIC             "Country_Code",
# MAGIC             "Demand_Location",
# MAGIC             "Fulfilling_Location",
# MAGIC             "Jda_Order AS Original_Jda_Order",
# MAGIC             "Warehouse_Reason_Code",
# MAGIC             "Warehouse_Reason",
# MAGIC             "Customer_Reason_Code",
# MAGIC             "Customer_Reason",
# MAGIC             "Currency",
# MAGIC             "Webstore"
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

df: DataFrame
orders_df: DataFrame
order_cancels_df: DataFrame

shop_json_path: str = azure_storage_demand_inbound_path_format.format("*-Cancel-*.json")
try:
    df: DataFrame = (
    spark.read.format("json")
    .schema(json_schema)
    .load(shop_json_path)
    .withColumn("__fileName", spark_functions.input_file_name())
)
    orders_df: DataFrame = (df.withColumn("orders", spark_functions.explode("orders"))
                            .select("orders.*", "__fileName")
                            .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
                            .withColumn("Order_Total", spark_functions.col("Order_Total").cast(spark_types.DoubleType()))
                            .withColumn("Empl_Id", spark_functions.lit(None).cast(spark_types.StringType()))
                            .withColumn("Jda_Order", spark_functions.lit(-1).cast(spark_types.StringType()))
                            .withColumn("Status", spark_functions.lit("X"))
                            .withColumn("Status_Text", spark_functions.lit("Cancelled"))
                            .withColumn("Type", spark_functions.when(spark_functions.col("Type") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Type")))
                            .withColumn("Status_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Status_Date")))
                            .withColumn("Order_Demand_Amt", spark_functions.col("Order_Demand_Amt").cast(spark_types.DoubleType()))
                            .withColumn("Upc", spark_functions.when(spark_functions.col("Upc") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Upc")))
                            .withColumn("Line_Qty", spark_functions.col("Line_Qty").cast(spark_types.DoubleType()))
                            .withColumn("Line_Sub_Total", spark_functions.col("Line_Sub_Total").cast(spark_types.DoubleType()))
                            .withColumn("Tender_2", spark_functions.when(spark_functions.col("Tender_2") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Tender_2")))
                            .withColumn("Tender_3", spark_functions.when(spark_functions.col("Tender_3") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Tender_3")))
                            .withColumn("Customer_Ip_Add", spark_functions.lit(None).cast(spark_types.StringType()))
                            .withColumn("Order_Entry_Method", spark_functions.lit("Website"))
                            .withColumn("Csr_Associate_Name", spark_functions.lit(None).cast(spark_types.StringType()))
                            .withColumn("Fulfilling_Location", spark_functions.lit(None).cast(spark_types.StringType()))
                            .withColumn("Warehouse_Reason_Code", spark_functions.when(spark_functions.col("Warehouse_Reason_Code") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Warehouse_Reason_Code")))
                            .withColumn("Warehouse_Reason", spark_functions.when(spark_functions.col("Warehouse_Reason") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Warehouse_Reason")))
                            .withColumn("Customer_Reason_Code", spark_functions.when(spark_functions.col("Customer_Reason_Code") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Customer_Reason_Code")))
                            .withColumn("Customer_Reason", spark_functions.when(spark_functions.col("Customer_Reason") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Customer_Reason")))
                            .withColumn("Currency", spark_functions.when(spark_functions.col("Currency") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Currency")))
                        )
except:
    orders_df = None
    print("order_cancels_df is None")

if orders_df is not None:
    order_cancels_df: DataFrame = (
    orders_df
    .alias("cancel")
    .join(
        baozun_tender_mapping_df.alias("tender"),
        how="left",
        on=[spark_functions.col("cancel.Country_Code") == spark_functions.col("tender.Country"), spark_functions.col("cancel.Tender_1") == spark_functions.col("tender.TenderID")]
    )
    .selectExpr(
        "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
        "FORMAT_STRING('%.2f', Order_Total) AS Order_Total",
        "Store",
        "COALESCE(Empl_Id, 'NULL') AS Empl_Id",
        "Jda_Order",
        "Line",
        "Status",
        "Status_Text",
        "COALESCE(Type, 'NULL') AS Type",
        "Transaction_Nbr AS Weborder_Id",
        "DATE_FORMAT(Status_Date, 'yyyy-MM-dd HH:mm:ss') AS Status_Date",
        "FORMAT_STRING('%.2f', Order_Demand_Amt) AS Order_Demand_Amt",
        "COALESCE(Upc, 'NULL') AS Upc",
        "APAC_style",
        "APAC_colour",
        "APAC_size",
        "FORMAT_STRING('%.2f', Line_Qty) AS Line_Qty",
        "FORMAT_STRING('%.2f', Line_Sub_Total) AS Line_Sub_Total",
        "Disc_Promo",
        "COALESCE(tender.TenderType, 'OTHER') AS Tender_1",
        "COALESCE(Tender_2, 'NULL') AS Tender_2",
        "COALESCE(Tender_3, 'NULL') AS Tender_3",
        "COALESCE(Customer_Ip_Add, 'NULL') AS Customer_Ip_Add",
        "Order_Entry_Method",
        "COALESCE(Csr_Associate_Name, 'NULL') AS Csr_Associate_Name",
        "Country_Code",
        "Demand_Location",
        "COALESCE(Fulfilling_Location, 'NULL') AS Fulfilling_Location",
        "Jda_Order AS Original_Jda_Order",
        "COALESCE(Warehouse_Reason_Code, 'NULL') AS Warehouse_Reason_Code",
        "COALESCE(Warehouse_Reason, 'NULL') AS Warehouse_Reason",
        "COALESCE(Customer_Reason_Code, 'NULL') AS Customer_Reason_Code",
        "COALESCE(Customer_Reason, 'NULL') AS Customer_Reason",
        "COALESCE(Currency, 'NULL') AS Currency",
        "Webstore"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     order_cancels_dfs[region].coalesce(1).write.format("csv").options(**{
# MAGIC         "sep": "|",
# MAGIC         "lineSep": "\r\n",
# MAGIC         "encoding": "UTF-8",
# MAGIC         "quote": "",
# MAGIC         "escape": "",
# MAGIC         "header": True,
# MAGIC         "multiLine": False
# MAGIC     }).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, "cancels", from_order_date.strftime("%Y%m%d"))))
# MAGIC ```

# COMMAND ----------

if orders_df is not None:
    try:
        Out_File_date: datetime = datetime.strptime(
        order_cancels_df.selectExpr("MIN(status_Date) AS date").collect()[0]["date"],
        "%Y-%m-%d %H:%M:%S")
        print("Out_File_date:   ",Out_File_date)
    except:
        inbound_file_list = dbutils.fs.ls(
    azure_storage_demand_inbound_path_format.format("")
    )
        for i in range(len(inbound_file_list)):
    # Delete all JSON file in non-archive inbound path.
            if fnmatch.filter([inbound_file_list[i].name], "*-Cancel-*.json"):
                filename: str = inbound_file_list[i].name[3:][:8]
                Out_File_date = datetime.strptime(filename, "%Y%m%d")
                #Out_File_date = from_order_date
                print("filename:    ", filename)
                break
        print ("In catch:   ", Out_File_date)
    order_cancels_df.coalesce(1).write.format("csv").options(**{
    "sep": "|",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join("cancels", Out_File_date.strftime("%Y%m%d"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     inbound_file_list = dbutils.fs.ls(
# MAGIC         azure_storage_demand_inbound_path_format.format("/".join([region]))
# MAGIC     )
# MAGIC
# MAGIC     for i in range(len(inbound_file_list)):
# MAGIC         # Delete all JSON file in non-archive inbound path.
# MAGIC         if os.path.splitext(inbound_file_list[i].name)[-1].lower() == ".json":
# MAGIC             dbutils.fs.rm(
# MAGIC                 azure_storage_demand_inbound_path_format.format("/".join([region, inbound_file_list[i].name]))
# MAGIC             )
# MAGIC
# MAGIC     del i
# MAGIC ```

# COMMAND ----------

inbound_file_list = dbutils.fs.ls(
    azure_storage_demand_inbound_path_format.format("")
)

for i in range(len(inbound_file_list)):
    # Delete all JSON file in non-archive inbound path.
    if fnmatch.filter([inbound_file_list[i].name], "*-Cancel-*.json"):
        dbutils.fs.rm(
            azure_storage_demand_inbound_path_format.format(inbound_file_list[i].name)
        )
        print("Deleted: {0}".format(inbound_file_list[i].name))
del i


# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     outbound_temp_type_folder_list = dbutils.fs.ls(
# MAGIC         azure_storage_demand_outbound_temp_path_format.format(os.path.join(region))
# MAGIC     )
# MAGIC     
# MAGIC     for i in range(len(outbound_temp_type_folder_list)):
# MAGIC         outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
# MAGIC         outbound_temp_date_folder_list = dbutils.fs.ls(
# MAGIC             azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, outbound_temp_type_folder_name))
# MAGIC         )
# MAGIC
# MAGIC         file_data_type: str
# MAGIC         match outbound_temp_type_folder_name:
# MAGIC             case "headers":
# MAGIC                 # file_data_type = "Transaction"
# MAGIC                 continue
# MAGIC             case "lines":
# MAGIC                 # file_data_type = "Line"
# MAGIC                 continue
# MAGIC             case "payments":
# MAGIC                 # file_data_type = "Payment"
# MAGIC                 continue
# MAGIC             case "cancels":
# MAGIC                 file_data_type = "Cancel"
# MAGIC             case _:
# MAGIC                 continue
# MAGIC
# MAGIC         for j in range(len(outbound_temp_date_folder_list)):
# MAGIC             outbound_temp_date_folder_name: str = outbound_temp_date_folder_list[j].name.replace("/", "")
# MAGIC             outbound_temp_file_list = dbutils.fs.ls(
# MAGIC                 azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, outbound_temp_type_folder_name, outbound_temp_date_folder_name))
# MAGIC             )
# MAGIC
# MAGIC             for k in range(len(outbound_temp_file_list)):
# MAGIC                 outbound_temp_file_name: str = outbound_temp_file_list[k].name.replace("/", "")
# MAGIC
# MAGIC                 # Copy from temp to actual outbound location.
# MAGIC                 if os.path.splitext(outbound_temp_file_name)[-1].lower() == ".csv":
# MAGIC                     dbutils.fs.cp(
# MAGIC                         azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, outbound_temp_type_folder_name, outbound_temp_date_folder_name, outbound_temp_file_name)),
# MAGIC                         azure_storage_demand_outbound_path_format.format(os.path.join(region, outbound_temp_type_folder_name, "BI-RalphLauren_{0}_{1}-{2}-{3}.csv".format(region, file_data_type, job_time.strftime("%Y%m%d%H%M%S"), outbound_temp_date_folder_name)))
# MAGIC                     )
# MAGIC             del k
# MAGIC         del j
# MAGIC     del i
# MAGIC
# MAGIC     dbutils.fs.rm(
# MAGIC         azure_storage_demand_outbound_temp_path_format.format(os.path.join(region)),
# MAGIC         recurse=True
# MAGIC     )
# MAGIC ```

# COMMAND ----------

if orders_df is not None:
    print("order_cancels_df is not nonw")
    outbound_temp_type_folder_list = dbutils.fs.ls(
        azure_storage_demand_outbound_temp_path_format.format("")
    )

    for i in range(len(outbound_temp_type_folder_list)):
        outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
        outbound_temp_date_folder_list = dbutils.fs.ls(
            azure_storage_demand_outbound_temp_path_format.format(outbound_temp_type_folder_name)
        )

        file_data_type: str
        match outbound_temp_type_folder_name:
            case "headers":
                continue
            case "lines":
                continue
            case "payments":
                continue
            case "cancels":
                file_data_type = "Cancel"
            case _:
                continue

        for j in range(len(outbound_temp_date_folder_list)):
            outbound_temp_date_folder_name: str = outbound_temp_date_folder_list[j].name.replace("/", "")
            outbound_temp_file_list = dbutils.fs.ls(
                azure_storage_demand_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name, outbound_temp_date_folder_name))
            )

            for k in range(len(outbound_temp_file_list)):
                outbound_temp_file_name: str = outbound_temp_file_list[k].name.replace("/", "")

                # Copy from temp to actual outbound location.
                if fnmatch.filter([outbound_temp_file_name], "*.csv"):
                    dbutils.fs.cp(
                        azure_storage_demand_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name, outbound_temp_date_folder_name, outbound_temp_file_name)),
                        azure_storage_demand_outbound_path_format.format(os.path.join(outbound_temp_type_folder_name, "BI-RalphLauren_CN_HK_{0}-{1}.csv".format(file_data_type, outbound_temp_date_folder_name)))
                    )
            del k
        del j
    del i

    dbutils.fs.rm(
        azure_storage_demand_outbound_temp_path_format.format(""),
        recurse=True
    )
