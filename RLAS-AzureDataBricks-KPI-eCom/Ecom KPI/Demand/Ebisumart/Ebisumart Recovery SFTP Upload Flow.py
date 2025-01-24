# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is to upload files to SFTP for eBizumart Recovery
# MAGIC External dependencies
# MAGIC - PIP: paramiko

# COMMAND ----------

from collections import OrderedDict
import copy
from datetime import date, datetime, timedelta, timezone
import fnmatch
import json
import os
from typing import Dict, List, cast

import paramiko

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_recovery_path_prefix: str = "EcomKPI/Demand/Recovery"
demand_config_path_prefix: str = "EcomKPI/Demand/Config"
demand_eot_path_prefix: str = "EcomKPI/Demand/EOTFile"
local_temp_path_prefix: str = "/tmp/demand/recovery/ebisumart"

demand_path_outbound_suffix_format: str = os.path.join(demand_recovery_path_prefix, "Outbound/ebisumart/JP/{0}")
demand_path_archive_outbound_suffix_format: str = os.path.join(demand_recovery_path_prefix, "Archive/Outbound/ebisumart/JP/{0}")
local_demand_outbound_folder_path: str = os.path.join(local_temp_path_prefix, "outbound")

#sftp_server_name: str = "vpce-0ae0e2404c4bd2dea-8o2kuqsq.server.transfer.us-east-1.vpce.amazonaws.com"
sftp_server_name: str = dbutils.secrets.get("storagescope", "sftp-BI-AWS-host")
#sftp_port: int = 22
sftp_port: int = int(dbutils.secrets.get("storagescope", "sftp-BI-AWS-port"))
#sftp_user_name: str = "SFTP-QA-source-user-raw"
sftp_user_name: str = dbutils.secrets.get("storagescope", "sftp-BI-AWS-user")
sftp_path: str = "/rl-dna-qa-raw/landing/BizTalk-landing/APAC/JP.com/Demand"
local_private_key_path: str = os.path.join(local_temp_path_prefix, "privatekey.key")
local_eot_path: str = os.path.join(local_temp_path_prefix, "transfercomplete.EOT")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

azure_storage_demand_outbound_path_format: str = azure_storage_path_format.format(demand_path_outbound_suffix_format)

azure_storage_demand_archive_outbound_path_format: str = azure_storage_path_format.format(demand_path_archive_outbound_suffix_format)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

# Clean-up the local temp path
dbutils.fs.rm("file://{}".format(local_temp_path_prefix), recurse=True)

# COMMAND ----------

# Copy private key to local temp path
dbutils.fs.cp(azure_storage_path_format.format(os.path.join(demand_config_path_prefix, "privatekey.key")), "file://{}".format(local_private_key_path))

# COMMAND ----------

# Copy outbound file to local temp path
try:
    dbutils.fs.cp(azure_storage_path_format.format(demand_path_outbound_suffix_format.format("headers")), "file://{}".format(local_demand_outbound_folder_path), recurse=True)
    print("Header files copied")
except:
    print("No header files found")

try:
    dbutils.fs.cp(azure_storage_path_format.format(demand_path_outbound_suffix_format.format("lines")), "file://{}".format(local_demand_outbound_folder_path), recurse=True)
    print("Line files copied")
except:
    print("No line files found")

try:
    dbutils.fs.cp(azure_storage_path_format.format(demand_path_outbound_suffix_format.format("payments")), "file://{}".format(local_demand_outbound_folder_path), recurse=True)
    print("Payment files copied")
except:
    print("No payment files found")

try:
    dbutils.fs.cp(azure_storage_path_format.format(demand_path_outbound_suffix_format.format("cancels")), "file://{}".format(local_demand_outbound_folder_path), recurse=True)
    print("Cancel files copied")
except:
    print("No cancel files found")

# Copy EOT file to local temp path
dbutils.fs.cp(azure_storage_path_format.format(os.path.join(demand_eot_path_prefix, "Transfercomplete.EOT")), "file://{}".format(local_eot_path))

# COMMAND ----------

ssh: paramiko.SSHClient = paramiko.SSHClient()
ssh_private_key: paramiko.pkey.PKey = paramiko.rsakey.RSAKey.from_private_key_file(local_private_key_path, password=None)
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(
    sftp_server_name,
    port=sftp_port,
    username=sftp_user_name,
    # key_filename=local_private_key_path
    pkey=ssh_private_key,
    look_for_keys=False
)
sftp_client: paramiko.sftp_client.SFTPClient = ssh.open_sftp()

# COMMAND ----------

# Upload to SFTP
for name in [f for f in os.listdir(local_demand_outbound_folder_path) if os.path.isfile(os.path.join(local_demand_outbound_folder_path, f))]:
    if name.endswith(".csv"):
        sftp_client.put(os.path.join(local_demand_outbound_folder_path, name), os.path.join(sftp_path, name))
        print("Uploaded file {0}".format(name))
sftp_client.put(local_eot_path, os.path.join(sftp_path, "transfercomplete.EOT"))
print("Uploaded EOT file")

# COMMAND ----------

adls_output_folder_infos = dbutils.fs.ls(azure_storage_path_format.format(demand_path_outbound_suffix_format.format("")))

# Move all outbound files to archive
for adls_output_folder_info in adls_output_folder_infos:
    # Skip first level non-folder
    if not adls_output_folder_info.name.endswith("/"):
        continue

    folder_name: str = adls_output_folder_info.name.replace("/", "")
    adls_output_files = dbutils.fs.ls(azure_storage_path_format.format(demand_path_outbound_suffix_format.format(adls_output_folder_info.name)))

    for adls_output_file in adls_output_files:
        folder_file_path: str = os.path.join(folder_name, adls_output_file.name)
        dbutils.fs.mv(azure_storage_path_format.format(demand_path_outbound_suffix_format.format(folder_file_path)), azure_storage_demand_archive_outbound_path_format.format(os.path.join(job_time.strftime("%Y/%m/%d"), folder_file_path), recurse=True))

# COMMAND ----------

sftp_client.listdir(sftp_path)

# COMMAND ----------

# Clean-up the local temp path
dbutils.fs.rm("file://{}".format(local_temp_path_prefix), recurse=True)
