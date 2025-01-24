# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to process Baozun Normal Sales Flow
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

shop_ids: Dict[str, str] = {
    "CN": "141698",
    "TM": "10795",
    "JD": "10848",
    "HK": "596771"
}

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
cn_baozun_raw_json_schema_path: str = os.path.join(demand_mapping_path_prefix, "BaozunRawJsonSchema.json")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_demand_inbound_path_format: str = azure_storage_path_format.format(demand_path_inbound_suffix_format)
azure_storage_demand_outbound_temp_path_format: str = azure_storage_path_format.format(demand_path_outbound_temp_suffix_format)
azure_storage_demand_outbound_path_format: str = azure_storage_path_format.format(demand_path_outbound_suffix_format)

azure_storage_demand_archive_outbound_path_format: str = azure_storage_path_format.format(demand_path_archive_outbound_suffix_format)

azure_storage_baozun_raw_json_schema_path: str = azure_storage_path_format.format(cn_baozun_raw_json_schema_path)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_types

# COMMAND ----------

temp_cn_baozun_raw_json_schema_path: str = os.path.join(".", cn_baozun_raw_json_schema_path)
dbutils.fs.cp(azure_storage_baozun_raw_json_schema_path, temp_cn_baozun_raw_json_schema_path)

# json_schema_dict: Dict[str, object] = json.loads("""
# {"fields":[{"metadata":{},"name":"code","nullable":true,"type":"long"},{"metadata":{},"name":"message","nullable":true,"type":"string"},{"metadata":{},"name":"orders","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"Bill_To","nullable":true,"type":"string"},{"metadata":{},"name":"Bill_To_Country","nullable":true,"type":"string"},{"metadata":{},"name":"CoD_Charge","nullable":true,"type":"string"},{"metadata":{},"name":"CoD_Charge_Tax","nullable":true,"type":"string"},{"metadata":{},"name":"Country","nullable":true,"type":"string"},{"metadata":{},"name":"Coupon_Amount","nullable":true,"type":"string"},{"metadata":{},"name":"Customer_ID","nullable":true,"type":"string"},{"metadata":{},"name":"Demand_Net","nullable":true,"type":"string"},{"metadata":{},"name":"Device_Type","nullable":true,"type":"string"},{"metadata":{},"name":"Discount_Amount","nullable":true,"type":"string"},{"metadata":{},"name":"Employee_ID","nullable":true,"type":"string"},{"metadata":{},"name":"Employee_Purchase","nullable":true,"type":"string"},{"metadata":{},"name":"Freight_Amount","nullable":true,"type":"string"},{"metadata":{},"name":"Freight_Tax","nullable":true,"type":"string"},{"metadata":{},"name":"Line_Total","nullable":true,"type":"string"},{"metadata":{},"name":"Order_Channel","nullable":true,"type":"string"},{"metadata":{},"name":"Order_Date","nullable":true,"type":"string"},{"metadata":{},"name":"Order_Lines","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"AX_Color_ID","nullable":true,"type":"string"},{"metadata":{},"name":"AX_Size_ID","nullable":true,"type":"string"},{"metadata":{},"name":"AX_Style_ID","nullable":true,"type":"string"},{"metadata":{},"name":"Demand_Net","nullable":true,"type":"string"},{"metadata":{},"name":"Demand_Sales_Unit","nullable":true,"type":"string"},{"metadata":{},"name":"Discount_Amount","nullable":true,"type":"string"},{"metadata":{},"name":"Gift_Charge","nullable":true,"type":"string"},{"metadata":{},"name":"Gift_IND","nullable":true,"type":"string"},{"metadata":{},"name":"Gift_Tax","nullable":true,"type":"string"},{"metadata":{},"name":"Item_Seq_Number","nullable":true,"type":"string"},{"metadata":{},"name":"Non_Merch_Item","nullable":true,"type":"string"},{"metadata":{},"name":"Order_Date","nullable":true,"type":"string"},{"metadata":{},"name":"Ship_To_Country","nullable":true,"type":"string"},{"metadata":{},"name":"Shipment_ID","nullable":true,"type":"string"},{"metadata":{},"name":"Shipment_Method","nullable":true,"type":"string"},{"metadata":{},"name":"Transaction_Nbr","nullable":true,"type":"string"},{"metadata":{},"name":"eCom_Store_ID","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"Order_Payments","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"Demand_Currency","nullable":true,"type":"string"},{"metadata":{},"name":"Order_Date","nullable":true,"type":"string"},{"metadata":{},"name":"Tender","nullable":true,"type":"string"},{"metadata":{},"name":"Tender_Amt","nullable":true,"type":"string"},{"metadata":{},"name":"Transaction_Nbr","nullable":true,"type":"string"},{"metadata":{},"name":"eCom_Store_ID","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"Promotion_Coupons","nullable":true,"type":"string"},{"metadata":{},"name":"Ship_To","nullable":true,"type":"string"},{"metadata":{},"name":"Shipping_Fee","nullable":true,"type":"string"},{"metadata":{},"name":"Shipping_Fee_Tax","nullable":true,"type":"string"},{"metadata":{},"name":"Tax","nullable":true,"type":"string"},{"metadata":{},"name":"Transaction_Nbr","nullable":true,"type":"string"},{"metadata":{},"name":"eCom_Store_ID","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"pageSize","nullable":true,"type":"long"},{"metadata":{},"name":"pages","nullable":true,"type":"long"},{"metadata":{},"name":"success","nullable":true,"type":"boolean"},{"metadata":{},"name":"total","nullable":true,"type":"long"}],"type":"struct"}
# """)

json_schema_dict: Dict[str, object]
with open("/dbfs/{}".format(temp_cn_baozun_raw_json_schema_path), "r") as f:
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

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC dfs: Dict[str, DataFrame] = {}
# MAGIC orders_dfs: Dict[str, DataFrame] = {}
# MAGIC order_headers_dfs: Dict[str, DataFrame] = {}
# MAGIC order_details_dfs: Dict[str, DataFrame] = {}
# MAGIC order_payments_dfs: Dict[str, DataFrame] = {}
# MAGIC
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     shop_json_path: str = azure_storage_demand_inbound_path_format.format("*-Sales-*.json")
# MAGIC     dfs[region]: DataFrame = (
# MAGIC         spark.read.format("json")
# MAGIC         .schema(json_schema)
# MAGIC         .load(shop_json_path)
# MAGIC         .withColumn("__fileName", spark_functions.input_file_name())
# MAGIC     )
# MAGIC     orders_dfs[region]: DataFrame = (dfs[region].withColumn("orders", spark_functions.explode("orders"))
# MAGIC                                      .select("orders.*", "__fileName")
# MAGIC                                      .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
# MAGIC                                      .withColumn("Device_Type", spark_functions.col("Device_Type").cast(spark_types.IntegerType()))
# MAGIC                                      .withColumn("Device_Type", spark_functions.when(
# MAGIC                                             spark_functions.col("Device_Type").between(1, 2),
# MAGIC                                             spark_functions.element_at(spark_functions.lit(["Desktop", "Mobile Phoe"]), spark_functions.col("Device_Type"))
# MAGIC                                         )
# MAGIC                                         .otherwise(spark_functions.col("Device_Type"))
# MAGIC                                       )
# MAGIC                                      .withColumn("Order_Channel", spark_functions.lit("Website"))
# MAGIC                                      .withColumn("Customer_ID", spark_functions.lit(-1))
# MAGIC                                      .withColumn("Freight_Tax", spark_functions.col("Freight_Tax").cast(spark_types.DoubleType()))
# MAGIC                                      .withColumn("Demand_Net", spark_functions.col("Demand_Net").cast(spark_types.DoubleType()))
# MAGIC                                      .withColumn("Employee_ID", spark_functions.when(spark_functions.col("Employee_ID") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Employee_ID")))
# MAGIC                                      .withColumn("Freight_Amount", spark_functions.col("Freight_Amount").cast(spark_types.DoubleType()))
# MAGIC                                      .withColumn("Discount_Amount", spark_functions.col("Discount_Amount").cast(spark_types.DoubleType()))
# MAGIC                                      .withColumn("Tax", spark_functions.col("Tax").cast(spark_types.DoubleType()))
# MAGIC                                     )
# MAGIC     order_headers_dfs[region]: DataFrame = orders_dfs[region].selectExpr(
# MAGIC         "eCom_Store_ID",
# MAGIC         "Transaction_Nbr",
# MAGIC         "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
# MAGIC         "Country",
# MAGIC         "Device_Type",
# MAGIC         "Employee_Purchase",
# MAGIC         "Order_Channel",
# MAGIC         "Customer_ID",
# MAGIC         "Ship_To",
# MAGIC         "Bill_To",
# MAGIC         "Promotion_Coupons",
# MAGIC         "ROUND(Freight_Tax, '0.00') AS Freight_Tax",
# MAGIC         "ROUND(Demand_Net, '0.00') AS Demand_Net",
# MAGIC         "Employee_ID",
# MAGIC         "Bill_To_Country",
# MAGIC         "ROUND(Freight_Amount, '0.00') AS Freight_Amount",
# MAGIC         "ROUND(Discount_Amount, '0.00') AS Discount_Amount",
# MAGIC         "ROUND(Coupon_Amount, '0.00') AS Coupon_Amount",
# MAGIC         "ROUND(Tax, '0.00') AS Tax"
# MAGIC     )
# MAGIC
# MAGIC     line_df: DataFrame = (
# MAGIC         orders_dfs[region]
# MAGIC         .withColumn("Order_Lines", spark_functions.explode("Order_Lines"))
# MAGIC         .select("Order_Lines.*")
# MAGIC         .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
# MAGIC         .withColumn("Demand_Net", spark_functions.col("Demand_Net").cast(spark_types.DoubleType()))
# MAGIC         .withColumn("Demand_Sales_Unit", spark_functions.col("Demand_Sales_Unit").cast(spark_types.IntegerType()))
# MAGIC         .withColumn("Shipment_Method", spark_functions.lit(None).cast(spark_types.StringType()))
# MAGIC         .withColumn("Gift_IND", spark_functions.when(spark_functions.col("Gift_IND") == spark_functions.lit(""), spark_functions.lit(0)).otherwise(spark_functions.lit(1)))
# MAGIC         .withColumn("Discount_Amount", spark_functions.col("Discount_Amount").cast(spark_types.DoubleType()))
# MAGIC     )
# MAGIC
# MAGIC     order_details_dfs[region]: DataFrame = line_df.selectExpr(
# MAGIC         "eCom_Store_ID",
# MAGIC         "Transaction_Nbr",
# MAGIC         "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
# MAGIC         "AX_Style_ID",
# MAGIC         "AX_Color_ID",
# MAGIC         "AX_Size_ID",
# MAGIC         "ROUND(Demand_Net, '0.00') AS Demand_Net",
# MAGIC         "Demand_Sales_Unit",
# MAGIC         "Shipment_ID",
# MAGIC         "Shipment_Method",
# MAGIC         "Ship_To_Country",
# MAGIC         "Gift_IND",
# MAGIC         "Non_Merch_Item",
# MAGIC         "Gift_Charge",
# MAGIC         "Gift_Tax",
# MAGIC         "Item_Seq_Number",
# MAGIC         "ROUND(Discount_Amount, '0.00') AS Discount_Amount"
# MAGIC     )
# MAGIC
# MAGIC     del line_df
# MAGIC
# MAGIC     payment_df: DataFrame = (
# MAGIC         orders_dfs[region]
# MAGIC         .withColumn("Order_Payments", spark_functions.explode("Order_Payments"))
# MAGIC         .select("Country", "Order_Payments.*")
# MAGIC         .withColumn("Tender_Amt", spark_functions.when(spark_functions.col("Tender_Amt") == spark_functions.lit(""), spark_functions.lit(0).cast(spark_types.DoubleType())).otherwise(spark_functions.col("Tender_Amt").cast(spark_types.DoubleType())))
# MAGIC         .alias("payment")
# MAGIC         .join(baozun_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("payment.Country") == spark_functions.col("tender.Country"), spark_functions.col("payment.Tender") == spark_functions.col("tender.TenderID")])
# MAGIC         .select("payment.*", "tender.TenderType")
# MAGIC         .withColumn("TenderType", spark_functions.coalesce(spark_functions.col("TenderType"), spark_functions.lit("OTHER")))
# MAGIC     )
# MAGIC
# MAGIC     order_payments_dfs[region]: DataFrame = payment_df.selectExpr(
# MAGIC         "eCom_Store_ID",
# MAGIC         "Transaction_Nbr",
# MAGIC         "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
# MAGIC         "TenderType AS Tender",
# MAGIC         "Tender_Amt",
# MAGIC         "Demand_Currency"
# MAGIC     )
# MAGIC
# MAGIC     del payment_df
# MAGIC ```

# COMMAND ----------

df: DataFrame
orders_df: DataFrame
order_headers_df: DataFrame
order_details_df: DataFrame
order_payments_df: DataFrame

shop_json_path: str = azure_storage_demand_inbound_path_format.format("*-Sales-*.json")
df: DataFrame = (
    spark.read.format("json")
    .schema(json_schema)
    .load(shop_json_path)
    .withColumn("__fileName", spark_functions.input_file_name())
)
orders_df: DataFrame = (df.withColumn("orders", spark_functions.explode("orders"))
                                    .select("orders.*", "__fileName")
                                    .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
                                    .withColumn("Device_Type", spark_functions.col("Device_Type").cast(spark_types.IntegerType()))
                                    .withColumn("Device_Type", spark_functions.when(
                                        spark_functions.col("Device_Type").between(1, 2),
                                        spark_functions.element_at(spark_functions.lit(["Desktop", "Mobile Phone"]), spark_functions.col("Device_Type"))
                                    )
                                    .otherwise(spark_functions.col("Device_Type"))
                                    )
                                    .withColumn("Order_Channel", spark_functions.lit("Website"))
                                    .withColumn("Customer_ID", spark_functions.lit(-1))
                                    .withColumn("Promotion_Coupons", spark_functions.lit(None).cast(spark_types.StringType()))
                                    .withColumn("Freight_Tax", spark_functions.col("Freight_Tax").cast(spark_types.DoubleType()))
                                    .withColumn("Demand_Net", spark_functions.col("Demand_Net").cast(spark_types.DoubleType()))
                                    .withColumn("Employee_ID", spark_functions.when(spark_functions.col("Employee_ID") == spark_functions.lit(""), spark_functions.lit(None).cast(spark_types.StringType())).otherwise(spark_functions.col("Employee_ID")))
                                    .withColumn("Freight_Amount", spark_functions.col("Freight_Amount").cast(spark_types.DoubleType()))
                                    .withColumn("Discount_Amount", spark_functions.col("Discount_Amount").cast(spark_types.DoubleType()))
                                    .withColumn("Coupon_Amount", spark_functions.col("Coupon_Amount").cast(spark_types.DoubleType()))
                                    .withColumn("Tax", spark_functions.col("Tax").cast(spark_types.DoubleType()))
                                )
order_headers_df: DataFrame = orders_df.selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
    "Country",
    "Device_Type",
    "Employee_Purchase",
    "Order_Channel",
    "Customer_ID",
    "Ship_To",
    "Bill_To",
    "COALESCE(Promotion_Coupons, 'NULL') AS Promotion_Coupons",
    "FORMAT_STRING('%.2f', Freight_Tax) AS Freight_Tax",
    "FORMAT_STRING('%.2f', Demand_Net) AS Demand_Net",
    "COALESCE(Employee_ID, 'NULL') AS Employee_ID",
    "Bill_To_Country",
    "FORMAT_STRING('%.2f', Freight_Amount) AS Freight_Amount",
    "FORMAT_STRING('%.2f', Discount_Amount) AS Discount_Amount",
    "FORMAT_STRING('%.2f', Coupon_Amount) AS Coupon_Amount",
    "FORMAT_STRING('%.2f', Tax) AS Tax"
)

line_df: DataFrame = (
    orders_df
    .withColumn("Order_Lines", spark_functions.explode("Order_Lines"))
    .select("Order_Lines.*")
    .withColumn("Order_Date", spark_functions.to_timestamp_ntz(spark_functions.col("Order_Date")))
    .withColumn("Demand_Net", spark_functions.col("Demand_Net").cast(spark_types.DoubleType()))
    .withColumn("Demand_Sales_Unit", spark_functions.col("Demand_Sales_Unit").cast(spark_types.IntegerType()))
    .withColumn("Shipment_Method", spark_functions.lit(None).cast(spark_types.StringType()))
    .withColumn("Gift_IND", spark_functions.when(spark_functions.col("Gift_IND") == spark_functions.lit(""), spark_functions.lit(0)).otherwise(spark_functions.lit(1)))
    .withColumn("Discount_Amount", spark_functions.col("Discount_Amount").cast(spark_types.DoubleType()))
)

order_details_df: DataFrame = line_df.selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
    "AX_Style_ID",
    "AX_Color_ID",
    "AX_Size_ID",
    # "FORMAT_STRING('%.2f', Demand_Net) AS Demand_Net",
    "Demand_Net",
    "Demand_Sales_Unit",
    "Shipment_ID",
    "COALESCE(Shipment_Method, 'NULL') AS Shipment_Method",
    "Ship_To_Country",
    "Gift_IND",
    "Non_Merch_Item",
    "Gift_Charge AS GiftChrg",
    "Gift_Tax AS GiftTax",
    "Item_Seq_Number AS ItmSeqNum",
    "FORMAT_STRING('%.2f', Discount_Amount) AS DiscountAmt"
)

del line_df

payment_df: DataFrame = (
    orders_df
    .withColumn("Order_Payments", spark_functions.explode("Order_Payments"))
    .select("Country", "Order_Payments.*")
    .withColumn("Tender_Amt", spark_functions.when(spark_functions.col("Tender_Amt") == spark_functions.lit(""), spark_functions.lit(0).cast(spark_types.DoubleType())).otherwise(spark_functions.col("Tender_Amt").cast(spark_types.DoubleType())))
    .alias("payment")
    .join(baozun_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("payment.Country") == spark_functions.col("tender.Country"), spark_functions.col("payment.Tender") == spark_functions.col("tender.TenderID")])
    .select("payment.*", "tender.TenderType")
    .withColumn("TenderType", spark_functions.coalesce(spark_functions.col("TenderType"), spark_functions.lit("OTHER")))
)

order_payments_df: DataFrame = payment_df.selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "DATE_FORMAT(Order_Date, 'yyyy-MM-dd HH:mm:ss') AS Order_Date",
    "TenderType AS Tender",
    "Tender_Amt",
    "Demand_Currency"
)

del payment_df

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC for region, shop_id in shop_ids.items():
# MAGIC     order_headers_dfs[region].coalesce(1).write.format("csv").options(**{
# MAGIC         "sep": "|",
# MAGIC         "lineSep": "\r\n",
# MAGIC         "encoding": "UTF-8",
# MAGIC         "quote": "",
# MAGIC         "escape": "",
# MAGIC         "header": True,
# MAGIC         "multiLine": False
# MAGIC     }).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, "headers", from_order_date.strftime("%Y%m%d"))))
# MAGIC
# MAGIC     order_details_dfs[region].coalesce(1).write.format("csv").options(**{
# MAGIC         "sep": "|",
# MAGIC         "lineSep": "\r\n",
# MAGIC         "encoding": "UTF-8",
# MAGIC         "quote": "",
# MAGIC         "escape": "",
# MAGIC         "header": True,
# MAGIC         "multiLine": False
# MAGIC     }).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, "lines", from_order_date.strftime("%Y%m%d"))))
# MAGIC
# MAGIC     order_payments_dfs[region].coalesce(1).write.format("csv").options(**{
# MAGIC         "sep": "|",
# MAGIC         "lineSep": "\r\n",
# MAGIC         "encoding": "UTF-8",
# MAGIC         "quote": "",
# MAGIC         "escape": "",
# MAGIC         "header": True,
# MAGIC         "multiLine": False
# MAGIC     }).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join(region, "payments", from_order_date.strftime("%Y%m%d"))))
# MAGIC ```

# COMMAND ----------


try:
    Out_File_date: datetime = datetime.strptime(
    order_headers_df.selectExpr("MIN(Order_Date) AS date").collect()[0]["date"],
    "%Y-%m-%d %H:%M:%S")
    print (Out_File_date)
    #print (from_order_date)
except:
    inbound_file_list = dbutils.fs.ls(
    azure_storage_demand_inbound_path_format.format("")
    )
    for i in range(len(inbound_file_list)):
    # Delete all JSON file in non-archive inbound path.
        if fnmatch.filter([inbound_file_list[i].name], "*-Sales-*.json"):
            filename: str = inbound_file_list[i].name[3:][:8]
            Out_File_date = datetime.strptime(filename, "%Y%m%d")
            #Out_File_date = from_order_date
            print(filename)
            break
    print (Out_File_date)
order_headers_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join("headers", Out_File_date.strftime("%Y%m%d"))))

order_details_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join("lines", Out_File_date.strftime("%Y%m%d"))))

order_payments_df.coalesce(1).write.format("csv").options(**{
    "sep": ",",
    "lineSep": "\r\n",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "",
    "header": True,
    "multiLine": False
}).mode("overwrite").save(azure_storage_demand_outbound_temp_path_format.format(os.path.join("payments", Out_File_date.strftime("%Y%m%d"))))

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
    if fnmatch.filter([inbound_file_list[i].name], "*-Sales-*.json"):
        dbutils.fs.rm(
            azure_storage_demand_inbound_path_format.format("/".join([inbound_file_list[i].name]))
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
# MAGIC                 file_data_type = "Transaction"
# MAGIC             case "lines":
# MAGIC                 file_data_type = "Line"
# MAGIC             case "payments":
# MAGIC                 file_data_type = "Payment"
# MAGIC             case "cancels":
# MAGIC                 # Passed
# MAGIC                 # file_data_type = "Cancel"
# MAGIC                 continue
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
# MAGIC                 if fnmatch.filter([outbound_temp_file_name], "*.csv"):
# MAGIC                     dbutils.fs.cp(
# MAGIC                         azure_storage_demand_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name, outbound_temp_date_folder_name, outbound_temp_file_name)),
# MAGIC                         azure_storage_demand_outbound_path_format.format(os.path.join(outbound_temp_type_folder_name, "BI-RalphLauren_{0}_{1}-{2}-{3}.csv".format(region, file_data_type, job_time.strftime("%Y%m%d%H%M%S"), outbound_temp_date_folder_name)))
# MAGIC                     )
# MAGIC             del k
# MAGIC         del j
# MAGIC     del i
# MAGIC
# MAGIC     dbutils.fs.rm(
# MAGIC         azure_storage_demand_outbound_temp_path_format.format(""),
# MAGIC         recurse=True
# MAGIC     )
# MAGIC ```

# COMMAND ----------

outbound_temp_type_folder_list = dbutils.fs.ls(
    azure_storage_demand_outbound_temp_path_format.format("")
)

for i in range(len(outbound_temp_type_folder_list)):
    outbound_temp_type_folder_name: str = outbound_temp_type_folder_list[i].name.replace("/", "")
    outbound_temp_date_folder_list = dbutils.fs.ls(
        azure_storage_demand_outbound_temp_path_format.format(os.path.join(outbound_temp_type_folder_name))
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
            # Passed
            # file_data_type = "Cancel"
            continue
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

print("Completed moving files to Outbound folder")
