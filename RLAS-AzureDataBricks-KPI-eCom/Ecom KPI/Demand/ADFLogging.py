# Databricks notebook source
# MAGIC %run "/Workspace/Source/CommonUtilities/AppInsightsLogging"

# COMMAND ----------

# MAGIC %pip install azure-storage-queue
# MAGIC
# MAGIC import json
# MAGIC #from datetime import datetime, timedelta
# MAGIC #import pytz
# MAGIC from azure.storage.queue import QueueClient
# MAGIC #import uuid
# MAGIC def appLogging(transaction_id,hong_kong_time,sourcesystem,dest,interface,Notebook,ErrorMessage):
# MAGIC   #transaction_id = str(uuid.uuid4())
# MAGIC   #containername="adf"
# MAGIC   #hong_kong_tz = pytz.timezone('Asia/Hong_Kong')
# MAGIC   #hong_kong_time = datetime.now(hong_kong_tz)
# MAGIC   #sourcesystem="Baozun"
# MAGIC   #dest="AWS SFTP"
# MAGIC   #interface="eCom.KPI.Demand.Baozun"
# MAGIC   #Notebook="/Workspace/SourceKPI/Ecom KPI/Demand/Baozun/Baozun Normal Sales Download Flow"
# MAGIC   #ErrorMessage="Error"
# MAGIC   # storage_account_name = dbutils.secrets.get(scope,'azure-storage-accountname')
# MAGIC   # storage_account_access_key=dbutils.secrets.get(scope=scope,key="azure-storage-accountkey")
# MAGIC
# MAGIC   # Inbound json data
# MAGIC   source_json_data = {
# MAGIC       "CreatedTimeStamp": hong_kong_time.isoformat(),
# MAGIC       "TransactionID": transaction_id,
# MAGIC       "SourceSystem": sourcesystem,
# MAGIC       "TargetSystem": dest,
# MAGIC       "InterfaceName": interface,
# MAGIC       "Activity": "Receive",
# MAGIC       "EntryType": "Success",
# MAGIC       "ErrorMessage": ErrorMessage,
# MAGIC       "Attribute_1": Notebook,
# MAGIC       "Attribute_2": "",
# MAGIC       "Attribute_3": "",
# MAGIC       "Attribute_4": "",
# MAGIC       "Attribute_5": "",
# MAGIC       "Attribute_6": "",
# MAGIC       "Attribute_7": "",
# MAGIC       "Attribute_8": "",
# MAGIC       "Attribute_9": "",
# MAGIC       "Attribute_10": ""
# MAGIC   }
# MAGIC
# MAGIC   #print(source_json_data)
# MAGIC
# MAGIC   queue_name='adf-loganalytics-01'
# MAGIC   process_json_message(json.dumps(source_json_data), queue_name)
# MAGIC   print("Logged")
# MAGIC

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
import pytz

transaction_id = str(uuid.uuid4())
hong_kong_tz = pytz.timezone('Asia/Hong_Kong')
hong_kong_time = datetime.now(hong_kong_tz)
sourcesystem="Baozun API pass params"
Notebook="/Workspace/SourceKPI/Ecom KPI/Demand/Baozun/Baozun Normal Sales Download Flow"
interface="eCom.KPI.Demand.Baozun"
dest="AWS SFTP"
ErrorMessage="Error"

appLogging(transaction_id,hong_kong_time,sourcesystem,dest,interface,Notebook,ErrorMessage)
print("Logged")

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
import pytz
import json
from azure.storage.queue import QueueClient

def appLogging_v2(source, destination, interface, activity, entryType, notebook, error):
  transaction_id = str(uuid.uuid4())
  hong_kong_tz = pytz.timezone('Asia/Hong_Kong')
  hong_kong_time = datetime.now(hong_kong_tz)
  #sourcesystem="Baozun"
  #dest="AWS SFTP"
  #interface="eCom.KPI.Demand.Baozun"
  #Notebook="/Workspace/SourceKPI/Ecom KPI/Demand/Baozun/Baozun Normal Sales Download Flow"
  #ErrorMessage="Error"
  # storage_account_name = dbutils.secrets.get(scope,'azure-storage-accountname')
  # storage_account_access_key=dbutils.secrets.get(scope=scope,key="azure-storage-accountkey")

  # Inbound json data
  source_json_data = {
      "CreatedTimeStamp": hong_kong_time.isoformat(),
      "TransactionID": transaction_id,
      "SourceSystem": source,
      "TargetSystem": destination,
      "InterfaceName": interface,
      "Activity": activity,
      "EntryType": entryType,
      "ErrorMessage": error,
      "Attribute_1": notebook,
      "Attribute_2": "",
      "Attribute_3": "",
      "Attribute_4": "",
      "Attribute_5": "",
      "Attribute_6": "",
      "Attribute_7": "",
      "Attribute_8": "",
      "Attribute_9": "",
      "Attribute_10": ""
  }

  #print(source_json_data)

  queue_name='adf-loganalytics-01'
  process_json_message(json.dumps(source_json_data), queue_name)
  print("Logged")
