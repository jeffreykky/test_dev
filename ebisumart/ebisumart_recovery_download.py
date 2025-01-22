from datetime import datetime, timedelta, timezone
import os
import pathlib
import urllib.parse

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

# SET Widgets
dbutils.widgets.text("azure_storage_account_name", "rlasintegrationstoragdev", "Azure Storage Account Name")
dbutils.widgets.text("azure_storage_container_name", "adf", "Azure Storage Container Name")
dbutils.widgets.text("sharePointSite", "https://ralphlauren.sharepoint.com/sites/Biztalk-NonFinance/eComUserUpload", "SharePoint site:")
dbutils.widgets.text("sharePointPath", "Shared Documents/APAC eCom Dashboard Upload/eCom File transmission (Actual)/JP.com/Testing", "SharePoint folder path:")
dbutils.widgets.text("sharePointFileName", "*.csv", "")
dbutils.widgets.text("dbfsTempFolderPath", "/tmp/ebisumart/recovery", "")

# GET Widgets
sharepoint_site_uri: str = dbutils.widgets.get("sharePointSite")
sharepoint_path: str = dbutils.widgets.get("sharePointPath")
sharepoint_file_name: str = dbutils.widgets.get("sharePointFileName")
azure_storage_account_name: str = dbutils.widgets.get("azure_storage_account_name")
azure_storage_container_name: str = dbutils.widgets.get("azure_storage_container_name")

# GET Secrets
m365_user_name: str = dbutils.secrets.get("storagescope", "sharepoint-rl-username")
m365_password: str = dbutils.secrets.get("storagescope", "sharepoint-rl-password")
azure_storage_sas_token: str = dbutils.secrets.get("storagescope", "azure-storage-sastoken")
spark.conf.set(f"fs.azure.sas.{azure_storage_account_name}.{azure_storage_container_name}.blob.core.windows.net", azure_storage_sas_token)

# Constants
project_path_prefix: str = "EcomKPI/Demand"
flow = "Recovery"
job_time: datetime = datetime.now(timezone(timedelta(hours=8)))
dbfs_temp_folder_path: str = "/tmp/ebisumart/recovery"
pathlib.Path(dbfs_temp_folder_path).mkdir(parents=True, exist_ok=True)

# Azure Storage Paths
azure_storage_path_prefix: str = f"wasbs://{azure_storage_account_name}@{azure_storage_container_name}.blob.core.windows.net"
jp_ebisumart_inbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, flow, "Inbound/ebisumart/JP")
jp_ebisumart_archive_inbound_path: str = os.path.join(azure_storage_path_prefix, project_path_prefix, flow, "Archive/Inbound/ebisumart/JP")

# SharePoint Authentication
m365_authn_context: AuthenticationContext = AuthenticationContext(sharepoint_site_uri)
token = m365_authn_context.acquire_token_for_user(username=m365_user_name, password=m365_password)
sharepoint_client_context = ClientContext(sharepoint_site_uri, token)
sharepoint_client_context.web.get().execute_query()



sharepoint_folder = sharepoint_client_context.web.get_folder_by_server_relative_path(sharepoint_path)
sharepoint_files = sharepoint_folder.get_files(recursive=False).execute_query()

if len(sharepoint_files) == 0:
    print("No files found")

for file in sharepoint_files:
    file_link = file.get_absolute_url().execute_query().value
    file_uri = urllib.parse.urlparse(file_link)
    file_name: str = os.path.basename(urllib.parse.unquote(file_uri.path))
    pattern = r".*\.csv$"
    if not re.match(pattern, file_name):
        continue

    download_path: str = os.path.join(dbfs_temp_folder_path, file_name)
    with open(download_path, "wb") as local_file:
        file.download(local_file).execute_query()

    # Copy to Azure Storage
    dbutils.fs.cp(f"file://{download_path}", os.path.join(jp_ebisumart_inbound_path, file_name))
    print(f"File {file_name} downloaded.")

    # delete file from SharePoint Online after downloaded
    if file is not None:
        file.recycle().execute_query()
        print(f"Deleted file {file_name} from SharePoint Online")