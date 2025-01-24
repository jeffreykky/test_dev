# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to download data for Baozun Normal Sales Flow
# MAGIC External dependencies
# MAGIC - PIP: requests (already pre-installed in Databricks Runtime)

# COMMAND ----------

# MAGIC %run "./Baozun Web Request AES Encryption Helper"

# COMMAND ----------

from collections import OrderedDict
import copy
from datetime import date, datetime, timedelta, timezone
import json
import os
from typing import Dict, List, cast

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")
#dbutils.widgets.text("baozun_web_request_host", "https://hub4-bi-prod.baozun.com/bi", "Baozun Web Request Host")
#dbutils.widgets.text("baozun_web_request_app_id", "6a93ab82285ef9097d819da8a99ebebf", "Baozun Web Request App ID")
#dbutils.widgets.text("baozun_web_request_app_secret", "Ga38vJKT064MEueL8dL8SK33aAlFcJsv39AT4S9C/EjT+mpR3ZRc43ftoUWJZZbhx4fN9jVYQ8QGMK/kF+uANw==", "Baozun Web Request App Secret")
dbutils.widgets.text("baozun_data_start_time", "", "Baozun data start date (yyyy-MM-dd HH:mm:ss)")
dbutils.widgets.text("baozun_data_end_time", "", "Baozun data end date (yyyy-MM-dd HH:mm:ss)")

# COMMAND ----------

# Default value for normal flow
now: datetime = datetime.now(timezone(timedelta(hours=8)))

# Or, choose a fixed datetime
# year: int = 2024
# month: int = 5
# day: int = 1
# now: datetime = datetime(year, month, day, tzinfo=timezone(timedelta(hours=8)))

from_order_datetime_param: str = dbutils.widgets.get("baozun_data_start_time")
to_order_datetime_param: str = dbutils.widgets.get("baozun_data_end_time")

try:
    to_order_datetime: datetime = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo) if len(to_order_datetime_param.strip()) == 0 else datetime.strptime(to_order_datetime_param, "%Y-%m-%d %H:%M:%S")
except ValueError as e:
    print("Setting to_order_datetime as default due to error occurred: {0}".format(e))
    to_order_datetime: datetime = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo)

try:
    from_order_datetime: datetime = to_order_datetime + timedelta(days=-1) if len(from_order_datetime_param.strip()) == 0 else datetime.strptime(from_order_datetime_param, "%Y-%m-%d %H:%M:%S")
except ValueError as e:
    print("Setting from_order_datetime as default due to error occurred: {0}".format(e))
    from_order_datetime: datetime = to_order_datetime + timedelta(days=-1)

# COMMAND ----------

def send_baozun_web_request(baozun_web_request_config: Dict[str, str], request: Dict[str, object]) -> str:
    import requests
    # import urllib


    key: str = baozun_web_request_config.get("appSecret")
    ciphertext_in_bytes: bytes = encrypt_baozun_web_request(key, json.dumps(request))
    ciphertext: str = ciphertext_in_bytes.decode("utf-8")
    signing_params: Dict[str, str] = {
        "appSecret": key,
        "interfaceType": baozun_web_request_config.get("interfaceType"),
        "methodName": baozun_web_request_config.get("methodName"),
        "requestTime": baozun_web_request_config.get("requestTime"),
        "sourceApp": baozun_web_request_config.get("sourceApp"),
        "version": baozun_web_request_config.get("version"),
        "ciphertext": ciphertext
    }
    signed_request: str = sign_baozun_web_request_ciphertext(**signing_params)

    query_params: Dict[str, str] = {
        "requestTime": baozun_web_request_config.get("requestTime"),
        "sign": signed_request,
        "version": baozun_web_request_config.get("version"),
        "methodName": baozun_web_request_config.get("methodName"),
        "sourceApp": baozun_web_request_config.get("sourceApp"),
        "interfaceType": baozun_web_request_config.get("interfaceType")
    }

    # uri: str = "{0}?{1}".format(baozun_web_request_config.get("host"), urllib.parse.urlencode(query_params)) if len(query_params) > 0 else baozun_web_request_config.get("host")

    response: requests.Response = requests.post(
        baozun_web_request_config.get("host"),
        params=query_params,
        data=ciphertext
    )

    if response.status_code != 200:
        raise Exception(response.text)

    response_bytes: bytes = response.content
    if response_bytes[0:1] == b"<":
        import urllib.error
        raise urllib.error.HTTPError(response.url, 400, response.text, response.headers, response.content)

    # request_obj: requests.PreparedRequest = response.request
    # print("{0}\r\n{1}\r\n\r\n{2}".format(
    #     " ".join([request_obj.method, request_obj.url]),
    #     "\r\n".join("{0}: {1}".format(k, v) for k, v in request_obj.headers.items()),
    #     request_obj.body
    # ))

    # raw_response_content: str = decrypt_baozun_web_response(key, response_bytes)
    response_str: str = response_bytes.decode("utf-8")
    return response_str

# COMMAND ----------

# baozun_web_request_config: Dict[str, str] = {
#     "host": "https://hub4-bi-uat.baozun.com/bi",
#     "version": "1.0",
#     "methodName": "RL_demand_query",
#     "sourceApp": "53a5ed1051482fb8a8d328b98000288f",
#     "interfaceType": "1",
#     "appSecret": "jPHyonX8dVUewVBKzn8l86fsrF6oxIUkUgbZ6/yRPtCzILYZHgjxzGrt0pt+WfKWM9UMPbMZS/kmrGxX3kHNKw==",
#     "pageSize": 200
# }

baozun_web_request_config: Dict[str, str] = {
    #"host": dbutils.widgets.get("baozun_web_request_host"),
    "host": dbutils.secrets.get('storagescope', key="api-Baozun-host"),
    "version": "1.0",
    "methodName": "RL_demand_query",
    #"sourceApp": dbutils.widgets.get("baozun_web_request_app_id"),
    "sourceApp": dbutils.secrets.get('storagescope', key="api-Baozun-appID"),
    "interfaceType": "1",
    #"appSecret": dbutils.widgets.get("baozun_web_request_app_secret"),
    "appSecret": dbutils.secrets.get('storagescope', key="api-Baozun-secret"),
    "pageSize": 200
}

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
demand_path_inbound_suffix_format: str = os.path.join(demand_normal_path_prefix, "Inbound/baozun/{0}")
demand_path_archive_inbound_suffix_format: str = os.path.join(demand_normal_path_prefix, "Archive/Inbound/baozun/{0}")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_demand_inbound_path_format: str = azure_storage_path_format.format(demand_path_inbound_suffix_format)

azure_storage_demand_archive_inbound_path_format: str = azure_storage_path_format.format(demand_path_archive_inbound_suffix_format)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))
for region, shop_id in shop_ids.items():
    # Reset each shop page number to 1 from the beginning.
    page_number: int = 1
    while True:
        request: Dict[str, object] = {
            "pageNum": page_number,
            "pageSize": baozun_web_request_config["pageSize"],
            "shopId": shop_id,
            "startTime": from_order_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": to_order_datetime.strftime("%Y-%m-%d %H:%M:%S")
        }
        sign_params: Dict[str, object] = copy.deepcopy(baozun_web_request_config)
        sign_params["requestTime"] = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        response_json_str: str = send_baozun_web_request(sign_params, request)
        # print(response_json_str)

        # temp_folder_path: str = "./Demand/{}".format(region)
        temp_folder_path: str = "./Demand"
        temp_file_name: str = "{}-{}-Sales-{}.json".format(region, from_order_datetime.strftime("%Y%m%d"), page_number)
        temp_file_path: str = "/".join([temp_folder_path, temp_file_name])

        # Create parent folder
        os.makedirs(temp_folder_path, exist_ok=True)

        # Write to local temp JSON file
        with open(temp_file_path, "w") as f:
            f.write(response_json_str)
            f.flush()
            f.close()
        
        # Copy to Azure Storage
        dbutils.fs.cp(
            "file://{}".format(os.path.realpath(temp_file_path)),
            azure_storage_demand_inbound_path_format.format(temp_file_name)
        )
        dbutils.fs.cp(
            "file://{}".format(os.path.realpath(temp_file_path)),
            azure_storage_demand_archive_inbound_path_format.format(os.path.join(job_time.strftime("%Y/%m/%d"), temp_file_name))
        )

        # Remove local temp JSON file
        os.remove(temp_file_path)

        response_json: Dict[str, object] = json.loads(response_json_str)
        
        total_pages: int = cast(int, response_json.get("pages", 1))

        if total_pages > 1:
            page_number = page_number + 1
        
        if page_number == 1 or page_number > total_pages:
            break
