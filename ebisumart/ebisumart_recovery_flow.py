from datetime import date, datetime, timedelta, timezone
import os
from typing import Dict

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as spark_functions
from pyspark.sql.types import StructType, StructField, StringType


# SET Widgets
dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")
dbutils.widgets.dropdown("deduplicate", "Yes", ["Yes", "No"], "Deduplicate or not, default Yes")


# GET Widgets
azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
deduplicate: bool = dbutils.widgets.get("deduplicate") == "Yes"


# GET Secrets
azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{azure_storage_container_name}.{azure_storage_account_name}.blob.core.windows.net", azure_storage_sas_token)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# Constants
project_path_prefix: str = "EcomKPI/Demand"
flow = "Recovery"

# Azure Storage Paths
azure_storage_path_prefix: str = f"wasbs://{azure_storage_container_name}@{azure_storage_account_name}.blob.core.windows.net"
inbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, flow , "Inbound/ebisumart/JP")
outbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, "Outbound/ebisumart/JP")
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

# Read Recovery Files
inbound_schema: StructType = StructType([
    StructField("ORDER_DISP_NO", StringType(), False),
    StructField("ORDER_H_ACCESS_BROWSER_KBN", StringType(), False),
    StructField("ORDER_DATE", StringType(), False),
    StructField("KESSAI_ID", StringType(), False),
    StructField("MEMBER_ID", StringType(), True),
    StructField("SEIKYU", StringType(), False),
    StructField("SHIRE_SUM", StringType(), False),
    StructField("TEIKA_SUM", StringType(), False),
    StructField("TEIKA_SUM_TAX", StringType(), False),
    StructField("TAX", StringType(), False),
    StructField("SORYO", StringType(), False),
    StructField("SORYO_TAX", StringType(), False),
    StructField("DAIBIKI", StringType(), False),
    StructField("DAIBIKI_TAX", StringType(), False),
    StructField("ORDER_D_FREE_ITEM31", StringType(), False),
    StructField("ORDER_D_NO", StringType(), False),
    StructField("ORDER_D_FREE_ITEM41", StringType(), False),
    StructField("ORDER_D_FREE_ITEM32", StringType(), False),
    StructField("ORDER_D_TEIKA", StringType(), False),
    StructField("ORDER_D_SHIRE_PRICE", StringType(), False),
    StructField("ORDER_D_QUANTITY", StringType(), False),
    StructField("ORDER_D_FREE_ITEM35", StringType(), True),
    StructField("FREE_ITEM33", StringType(), True),
    StructField("COUPON_WARIBIKI", StringType(), True),
    StructField("COUPON_ID", StringType(), True),
    StructField("SHIP_SLIP_NO", StringType(), True),
    StructField("FREE_ITEM_11", StringType(), True)
])


inbound_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(inbound_schema).load(os.path.join(inbound_path, "FY*.csv"))

parsed_inbound_df: DataFrame = (inbound_df
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
                                spark_functions.to_timestamp(spark_functions.col("ORDER_DATE"), "yyyy/MM/dd H:m:s"),
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

header_final: DataFrame = parsed_inbound_df.select(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "Country",
    "Device_Type",
    "Employee_Purchase",
    "Order_Channel",
    "Customer_ID",
    "Ship_To",
    "Bill_To",
    "Promotion_Coupons",
    "Freight_Tax",
    "Demand_Net",
    "Employee_ID",
    "Bill_To_Country",
    "Freight_Amount",
    "Discount_Amount",
    "Coupon_Amount",
    "Tax",
    "CoD_Charge",
    "CoD_Charge_Tax",
    "Shipping_Fee",
    "Shipping_Fee_Tax",
    "Line_Total"
)

payment_final: DataFrame = parsed_inbound_df.alias("payment").join(ebisumart_tender_mapping_df.alias("tender"), how="left", on=[spark_functions.col("payment.Tender_ID") == spark_functions.col("tender.TenderID")]).selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "TenderType AS Tender",
    "Tender_Amt",
    "Demand_Currency"
)

# COMMAND ----------

line_df: DataFrame = (inbound_df
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
                              spark_functions.to_timestamp(spark_functions.col("ORDER_DATE"), "yyyy/MM/dd H:m:s"),
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

line_final: DataFrame = line_df.alias("line")\
                        .join(item_mapping_df.alias("item"), how="left", on=[spark_functions.col("line.Barcode") == spark_functions.col("item.Barcode")]
).selectExpr(
    "eCom_Store_ID",
    "Transaction_Nbr",
    "Order_Date",
    "item.Style AS AX_Style_ID",
    "item.Color AS AX_Color_ID",
    "item.Size AS AX_Size_ID",
    "line.Demand_Net",
    "Demand_Sales_Unit",
    "Shipment_ID",
    "Shipment_Method",
    "Ship_To_Country",
    "Gift_IND",
    "Non_Merch_Item",
    "Gift_Charge AS GiftChrg",
    "Gift_Tax AS GiftTax",
    "Item_Seq_Number AS ItmSeqNum",
    "line.Discount_Amount AS DiscountAmt"
)


print("Completed: Header, Payment, and Line")

# Read Inbound Cancel
inbound_cancel_schema: StructType = StructType([
    StructField("ORDER_DISP_NO", StringType(), False),
    StructField("ORDER_H_ACCESS_BROWSER_KBN", StringType(), False),
    StructField("ORDER_DATE", StringType(), False),
    StructField("KESSAI_ID", StringType(), False),
    StructField("MEMBER_ID", StringType(), True),
    StructField("SEIKYU", StringType(), False),
    StructField("SHIRE_SUM", StringType(), False),
    StructField("TEIKA_SUM", StringType(), False),
    StructField("TEIKA_SUM_TAX", StringType(), False),
    StructField("TAX", StringType(), False),
    StructField("SORYO", StringType(), False),
    StructField("SORYO_TAX", StringType(), False),
    StructField("DAIBIKI", StringType(), False),
    StructField("DAIBIKI_TAX", StringType(), False),
    StructField("ORDER_D_FREE_ITEM31", StringType(), False),
    StructField("ORDER_D_NO", StringType(), False),
    StructField("ORDER_D_FREE_ITEM41", StringType(), False),
    StructField("ORDER_D_FREE_ITEM32", StringType(), False),
    StructField("ORDER_D_TEIKA", StringType(), False),
    StructField("ORDER_D_SHIRE_PRICE", StringType(), False),
    StructField("ORDER_D_QUANTITY", StringType(), False),
    StructField("ORDER_D_FREE_ITEM35", StringType(), True),
    StructField("FREE_ITEM33", StringType(), True),
    StructField("COUPON_WARIBIKI", StringType(), True),
    StructField("COUPON_ID", StringType(), True),
    StructField("SHIP_SLIP_NO", StringType(), True),
    StructField("CANCEL_DATE", StringType(), True),
    StructField("SEND_DATE", StringType(), True)
])

# Start Processing Cancel
inbound_cancel_df: DataFrame = spark.read.format("csv").options(**{
    "sep": ",",
    "encoding": "UTF-8",
    "quote": "",
    "escape": "\"",
    "header": True,
    "inferSchema": False,
    "multiLine": False
}).schema(inbound_cancel_schema).load(os.path.join(inbound_path, "JP_cancellation*.csv"))


renamed_cancel_df: DataFrame = (inbound_cancel_df
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

header_df: DataFrame = renamed_cancel_df.select("eCom_Store_ID", "Transaction_Nbr", "Order_Date", "Cancel_Date", 
                                "Country", "Device_Type", "Employee_Purchase", "Order_Channel", "Customer_ID", 
                                "Shipment_ID", "Shipment_Method", "Ship_To_Country", "Ship_To", "Bill_To", 
                                "Promotion_Coupons", "Freight_Tax", "Demand_Gross", "Cancel_Whole_Order_Demand_Total",
                                "Demand_Net", "Employee_ID", "Bill_To_Country", "Demand_Currency", "Freight_Amount", 
                                "Discount_Amount", "Coupon_Amount", "Tax", "CoD_Charge", "CoD_Charge_Tax", "Shipping_Fee", 
                                "Shipping_Fee_Tax", "Line_Total", "Tender_ID", "Tender_Amt", "Item_Type", "Gift_Charge", 
                                "Gift_Tax", "Jda_Order"
                            )

line_df: DataFrame = (inbound_cancel_df
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

demand_recovery_cancel_header_df: DataFrame = renamed_cancel_df.filter("CANCEL_DATE IS NOT NULL")
cancel_final: DataFrame = (demand_recovery_cancel_header_df.alias("header")
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

write_to_delta_table(header_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"], deduplicate=deduplicate)
write_to_delta_table(payment_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"], deduplicate=deduplicate)
write_to_delta_table(line_final, ["eCom_Store_ID", "Transaction_Nbr", "Order_Date"], deduplicate=deduplicate)
write_to_delta_table(cancel_final, ["Store", "Weborder_Id", "Order_Date"], deduplicate=deduplicate)


# Archive Inbound files
# Step 1: List the files in the inbound path
inbound_files = dbutils.fs.ls(inbound_path)

# Step 2: Iterate over the file list
for file_info in inbound_files:
    # Check if the file has a .csv extension
    if file_info.name.lower().endswith(".csv"):
        # Construct the source and destination paths
        source_path = os.path.join(inbound_path, file_info.name)
        archive_path = os.path.join(archive_inbound_path,job_time.strftime("%Y/%m/%d"),file_info.name)
        # Archive the file
        dbutils.fs.mv(source_path, archive_path)