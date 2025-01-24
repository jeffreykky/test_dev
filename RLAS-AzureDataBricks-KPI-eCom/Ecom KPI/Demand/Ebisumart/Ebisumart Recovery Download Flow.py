# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for processing Recovery flow of eCom KPI Demand from Ebisumart (SharePoint Online)
# MAGIC Data includes
# MAGIC - Demand headers
# MAGIC - Demand lines
# MAGIC - Demand payments
# MAGIC - Cancel lines
# MAGIC
# MAGIC External dependencies
# MAGIC - ~~Maven coordinate `com.crealytics:spark-excel_2.12:3.4.1_0.20.3` for reading/writing Excel files (requires to update the version if the Spark version is different)~~
# MAGIC - Office365-REST-Python-Client

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-level flow
# MAGIC 1. Download file from SharePoint Online
# MAGIC 2. Move the file to Azure Storage Account (both Inbound and Archive)

# COMMAND ----------

dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")

# COMMAND ----------

#dbutils.widgets.text("m365_username", "app-ecom-auto@ralphlauren.com", "User Name:")
#dbutils.widgets.text("m365_password", "Fashion24", "Password:")
dbutils.widgets.text("sharePointSite", "https://ralphlauren.sharepoint.com/sites/Biztalk-NonFinance/eComUserUpload", "SharePoint site:")
dbutils.widgets.text("sharePointPath", "Shared Documents/APAC eCom Dashboard Upload/eCom File transmission (Actual)/JP.com/Testing", "SharePoint folder path:")
dbutils.widgets.text("sharePointFileName", "*.csv", "")
dbutils.widgets.text("dbfsTempFolderPath", "/tmp/ebisumart/recovery", "")

# COMMAND ----------

#m365_user_name: str = dbutils.widgets.get("m365_username")
m365_user_name: str = dbutils.secrets.get("storagescope", "sharepoint-rl-username")
#m365_password: str = dbutils.widgets.get("m365_password")
m365_password: str = dbutils.secrets.get("storagescope", "sharepoint-rl-password")
sharepoint_site_uri: str = dbutils.widgets.get("sharePointSite")
sharepoint_path: str = dbutils.widgets.get("sharePointPath")
sharepoint_file_name: str = dbutils.widgets.get("sharePointFileName")
dbfs_temp_folder_path: str = dbutils.widgets.get("dbfsTempFolderPath")

# COMMAND ----------

from datetime import datetime, timedelta, timezone
import os
import pathlib
# import tempfile
import urllib.parse

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

# COMMAND ----------

azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")
demand_recovery_path_prefix: str = "EcomKPI/Demand/Recovery"
jp_ebisumart_inbound_path: str = os.path.join(demand_recovery_path_prefix, "Inbound/ebisumart/JP")
jp_ebisumart_archive_inbound_path: str = os.path.join(demand_recovery_path_prefix, "Archive/Inbound/ebisumart/JP")

# COMMAND ----------

azure_storage_container_format: str = "wasbs://{1}@{0}.blob.core.windows.net/{{0}}"
azure_storage_path_format: str = azure_storage_container_format.format(azure_storage_account_name, azure_storage_container_name)

# COMMAND ----------

azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set("fs.azure.sas.{1}.{0}.blob.core.windows.net".format(azure_storage_account_name, azure_storage_container_name), azure_storage_sas_token)

# COMMAND ----------

job_time: datetime = datetime.now(timezone(timedelta(hours=8)))

# COMMAND ----------

# m365_user_credentials: UserCredential = UserCredential(m365_user_name, m365_password)
# sharepoint_client_context = ClientContext(sharepoint_site_uri).with_credentials(m365_user_credentials)
# print(sharepoint_client_context.base_url)

# COMMAND ----------

m365_authn_context: AuthenticationContext = AuthenticationContext(sharepoint_site_uri)
token = m365_authn_context.acquire_token_for_user(username=m365_user_name, password=m365_password)
sharepoint_client_context = ClientContext(sharepoint_site_uri, token)
try:
    result = sharepoint_client_context.web.get().execute_query()
    print(result)
except Exception as e:
    print("Error occurred:\n{0}".format(e))

# COMMAND ----------

# Create tmp path
pathlib.Path(dbfs_temp_folder_path).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

if sharepoint_file_name == "*.csv":
    try:
        sharepoint_jp_folder_object = sharepoint_client_context.web.get_folder_by_server_relative_path(sharepoint_path)
        sharepoint_jp_file_objects = sharepoint_jp_folder_object.get_files(recursive=False).execute_query()

        for sharepoint_jp_file_object in sharepoint_jp_file_objects:
            try:
                sharepoint_jp_file_link = sharepoint_jp_file_object.get_absolute_url().execute_query().value
                sharepoint_jp_file_uri = urllib.parse.urlparse(sharepoint_jp_file_link)
                sharepoint_jp_file_name: str = os.path.basename(urllib.parse.unquote(sharepoint_jp_file_uri.path))

                download_path: str = os.path.join(dbfs_temp_folder_path, sharepoint_jp_file_name)
                with open(download_path, "wb") as local_file:
                    sharepoint_jp_file_object.download(local_file).execute_query()

                # Copy to Azure Storage
                dbutils.fs.cp("file://{0}".format(download_path), azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_path, sharepoint_jp_file_name)))
                dbutils.fs.cp("file://{0}".format(download_path), azure_storage_path_format.format(os.path.join(jp_ebisumart_archive_inbound_path, job_time.strftime("%Y/%m/%d"), sharepoint_jp_file_name)))

                print("File {0} copied".format(sharepoint_jp_file_name))

                # delete file from SharePoint Online after downloaded
                if sharepoint_jp_file_object is not None:
                    sharepoint_jp_file_object.recycle().execute_query()
                    print("Removed file {0} from SharePoint Online".format(sharepoint_jp_file_name))
            except:
                if "sharepoint_jp_file_name" in locals():
                    print("Skipped file {0}".format(sharepoint_jp_file_name))
                elif "sharepoint_jp_file_uri" in locals():
                    print("Skipped file {0}".format(sharepoint_jp_file_uri))
                else:
                    print("Skipped file from SharePoint Online folder {0}".format(sharepoint_path))

        if len(sharepoint_jp_file_objects) > 0:
            print("Copied all files")
        else:
            print("No files copied")
    except Exception as ex:
        print(ex)
else:
    try:
        download_path: str = os.path.join(dbfs_temp_folder_path, os.path.basename(sharepoint_file_name))
        with open(download_path, "wb") as local_file:
            sharepoint_jp_file_object = (
                sharepoint_client_context.web.get_file_by_server_relative_path(os.path.join(sharepoint_path, sharepoint_file_name))
                .download(local_file)
                .execute_query()
            )

        # Copy to Azure Storage
        dbutils.fs.cp("file://{0}".format(download_path), azure_storage_path_format.format(os.path.join(jp_ebisumart_inbound_path, os.path.basename(sharepoint_file_name))))
        dbutils.fs.cp("file://{0}".format(download_path), azure_storage_path_format.format(os.path.join(jp_ebisumart_archive_inbound_path, job_time.strftime("%Y/%m/%d"), os.path.basename(sharepoint_file_name))))

        print("File {0} copied".format(sharepoint_file_name))

        # delete file from SharePoint Online after downloaded
        if sharepoint_jp_file_object is not None:
            sharepoint_jp_file_object.recycle().execute_query()
            print("Removed file {0} from SharePoint Online".format(sharepoint_file_name))
    except Exception as ex:
        print("Skipped file {0}".format(sharepoint_file_name))
        print(ex)
