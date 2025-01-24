
from collections import OrderedDict
import copy
from datetime import date, datetime, timedelta, timezone
import fnmatch
import json
import os
from typing import Dict, List, LiteralString, cast

import paramiko

# SET Widgets
dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")
dbutils.widgets.text("start_date", yesterday.strftime('%Y-%m-%d'), "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", yesterday.strftime('%Y-%m-%d'), "End Date (YYYY-MM-DD)")

# GET Widgets
azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")
start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
print(f"Start date (datetime): {start_date}, End date (datetime): {end_date}")

# GET Secrets
azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{azure_storage_container_name}.{azure_storage_account_name}.blob.core.windows.net", azure_storage_sas_token)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# Constants
job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# Azure Storage Paths
azure_storage_path_prefix: str = f"wasbs://{azure_storage_container_name}@{azure_storage_account_name}.blob.core.windows.net"
project_path_prefix: str = "EcomKPI/Demand"
outbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, "Outbound/ebisumart/JP")

local_temp_storage_path: str = "/tmp/demand/ebisumart/"
dbutils.fs.rm(f"file://{local_temp_storage_path}", recurse=True)


sftp_server_name: str = dbutils.secrets.get("storagescope", "sftp-BI-AWS-host")
sftp_port: int = int(dbutils.secrets.get("storagescope", "sftp-BI-AWS-port"))
sftp_user_name: str = dbutils.secrets.get("storagescope", "sftp-BI-AWS-user")
sftp_path: str = "/rl-dna-qa-raw/landing/BizTalk-landing/APAC/JP.com/Demand"

azure_private_key_path = os.path.join(azure_storage_path_prefix, "EcomKPI/Demand/Config/privatekey.key")
local_private_key_path: str = os.path.join(local_temp_storage_path, "privatekey.key")
dbutils.fs.cp(azure_private_key_path, local_private_key_path)



def get_sftp_client(host: str, port: int, username: str, private_key: str) -> paramiko.SFTPClient:
    
    ssh: paramiko.SSHClient = paramiko.SSHClient()
    ssh_private_key: paramiko.pkey.PKey = paramiko.rsakey.RSAKey.from_private_key_file(private_key, password=None)
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host,
        port=port,
        username=username,
        pkey=ssh_private_key,
        look_for_keys=False
    )
    return ssh.open_sftp()

def upload_table(table_name: LiteralString["header", "line", "payment", "cancel"], start_date: datetime= start_date, end_date: datetime= end_date):
    df = spark.table(f"ebisumart_{table_name}_history")
    df = df.filter(
        (col("ingestion_timestamp") >= start_date) & 
        (col("ingestion_timestamp") <= end_date)
    )
    df = df.toPandas()

    data_type_map = {
        "header": "Transaction",
        "line": "Line",
        "payment": "Payment",
        "cancel": "Cancel"
    }
    file_name = f"BI-RalphLauren_JP_{data_type_map[table_name]}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    local_path = os.path.join(sftp_path, file_name)
    upload_path = os.path.join(sftp_path, file_name)

    # Upload to SFTP
    df.to_csv(os.path.join(local_temp_storage_path, file_name), index=False)
    sftp_client.put(local_temp_storage_path, upload_path)
    print(f"Uploaded {table_name} to {upload_path}")
    # Archive file
    arhive_path = os.path.join(outbound_path, table_name, file_name)
    dbfs.fs.mv(local_path, arhive_path)


sftp_client = get_sftp_client(sftp_server_name, sftp_port, sftp_user_name, local_private_key_path)
for table_name in ["header", "line", "payment", "cancel"]:
    upload_table(table_name)
sftp_client.close()