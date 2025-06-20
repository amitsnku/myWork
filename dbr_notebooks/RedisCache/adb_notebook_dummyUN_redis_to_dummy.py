# Databricks notebook source
# Install the required libraries
%pip install azure-identity redis

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Listing scopes 

# COMMAND ----------

# dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Parameterizing the redis endpoint 

# COMMAND ----------

dbutils.widgets.text("adfglparam_askou_redis_endpoint_1", "")
dummyUN_redis_endpoint = dbutils.widgets.get("adfglparam_dummyUN_redis_endpoint_1")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Testing connection with Redis cache instance

# COMMAND ----------

# Step 2: Import the Redis library
import redis
import base64
import json
# Import modules
from azure.identity import ClientSecretCredential
from redis import Redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import json

def extract_username_from_token(token):
    parts = token.split('.')
    base64_str = parts[1]

    if len(base64_str) % 4 == 2:
        base64_str += "=="
    elif len(base64_str) % 4 == 3:
        base64_str += "="

    json_bytes = base64.b64decode(base64_str)
    json_str = json_bytes.decode('utf-8')
    jwt = json.loads(json_str)

    return jwt['oid']

# Step 1: Retrieve SPN Credentials from Databricks Secrets
client_id = dbutils.secrets.get(scope="dummyadbakvscope", key="un-ar-titan-redis-dummyUN-adb-client-id")  # Removed trailing space
client_secret = dbutils.secrets.get(scope="dummyadbakvscope", key="un-ar-titan-redis-dummyUN-adb-key")
tenant_id = dbutils.secrets.get(scope="dummyadbakvscope", key="un-ar-titan-redis-dummyUN-adb-tenant-id")

# Step 2: Define Redis details

redis_host=dummyUN_redis_endpoint

redis_port = 6380  # Default Redis TLS port

# Step 3: Authenticate using Azure AD (Service Principal)
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Step 4: Get OAuth token for Redis authentication
token = credential.get_token("https://redis.azure.com/.default")  # Correct scope for Redis AAD
user_name = extract_username_from_token(token.token)
pswd = token.token

# Step 5: Connect to Redis using AAD Authentication
try:
    redis_client = Redis(
        host=redis_host,
        port=redis_port,
        ssl=True,
        username=user_name,
        password=token.token,  # Use AAD token
        decode_responses=True  # Ensure responses are in string format
    )

    # Step 6: Test Redis Connection
    # redis_client.set("test-key", "Hello from Databricks using AAD!")
    # retrieved_value = redis_client.get("test-key")
    print(f"✅ Successfully connected to Redis:", redis_client.ping())

except Exception as e:
    # print(f"❌ Failed to connect to Redis: {str(e)}")
    # dbutils.notebook.exit(f"Failed to connect to Redis: {str(e)}")
    raise RuntimeError(f"❌ Failed to connect to Redis: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setting the pre-requisite folders, files and the paths.

# COMMAND ----------

import os 

landingzone_path = 'dbfs:/mnt/dummydatalake/DataSunrce/LandingZone/'
dummyUN_path = f'{landingzone_path}dummyUN/'
Redis_metadata_path = f'{dummyUN_path}Redis/metadata/'
mtdt_file = "redis_metadata.json"
json_path = Redis_metadata_path + mtdt_file
metadata_path = f'{Redis_metadata_path}{mtdt_file}'
metadata_key = "DateTimeUpdatedUtc"
temp_path=f'{dummyUN_path}Redis/temp/'
# temp_path='/dbfs/tmp/'
redis_path=f'{dummyUN_path}Redis/'

if 'dummyUN' not in [file.name for file in dbutils.fs.ls(landingzone_path)]:
    dbutils.fs.mkdirs(dummyUN_path)
    dbutils.fs.mkdirs(f'{dummyUN_path}Redis/')
    dbutils.fs.mkdirs(f'{dummyUN_path}Redis/metadata/')
    # dbutils.fs.mkdirs('dbfs:/mnt/dummydatalake/DataService/Projects/Student/dummyUN/Redis/metadata')

# Check if the directory exists and create it if it doesn't
if not any(file.name == 'metadata' for file in dbutils.fs.ls(f'{dummyUN_path}Redis/')):
    dbutils.fs.mkdirs(Redis_metadata_path)

# Check if the file exists
try:
    if not any(file.name == mtdt_file for file in dbutils.fs.ls(Redis_metadata_path)):
        # Create an empty JSON file
        dbutils.fs.put(json_path, '{"DateTimeUpdatedUtc":"2025-05-20 11:04:20"}', True)
        print("Empty JSON file created.")
    else:
        print("File already exists.")
except Exception as e:
    # dbutils.notebook.exit(f"metadata file redis_metadata.json not exist: {e}")
    raise RuntimeError(f"metadata file redis_metadata.json not exist: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### list_files_recursive function

# COMMAND ----------

def list_files_recursive(folder_path):
        file_info_list = dbutils.fs.ls(folder_path)
        all_files = []
        for file_info in file_info_list:
            if file_info.isDir():
                all_files.extend(list_files_recursive(file_info.path))
            else:
                all_files.append(file_info)
        return all_files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MetadataHandler 

# COMMAND ----------

class MetadataHandler:
    @staticmethod
    def read_metadata(metadata_path, default_metadata, key=None):
        try:
            # Check if the file exists using dbutils.fs
            if not any(file.name == "redis_metadata.json" for file in dbutils.fs.ls(metadata_path.rsplit('/', 1)[0])):
                dbutils.fs.put(metadata_path, json.dumps(default_metadata, indent=4), True)
                print("Metadata file initialized.")
                return default_metadata if key is None else {key: default_metadata.get(key)}
            else:
                metadata = json.loads(dbutils.fs.head(metadata_path))
                return metadata if key is None else {key: metadata.get(key)}
        except Exception as e:
            # dbutils.notebook.exit(f"Error initializing or reading metadata: {e}")
            raise RuntimeError(f"Error initializing or reading metadata: {e}")

    @staticmethod
    def update_metadata(metadata_path, key, new_value):
        try:
            metadata = json.loads(dbutils.fs.head(metadata_path))

            # Ensure new_value is a string before updating
            if isinstance(new_value, datetime):
                new_value = new_value.strftime('%Y-%m-%d %H:%M:%S')

            metadata[key] = new_value

            dbutils.fs.put(metadata_path, json.dumps(metadata, indent=4), True)
            print(f"Metadata updated for key '{key}': {new_value}")
        except Exception as e:
            # dbutils.notebook.exit(f"Error updating metadata key: {e}")
            raise RuntimeError(f"Error updating metadata key: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DataProcessor

# COMMAND ----------

from pyspark.sql import DataFrame
from datetime import datetime

class DataProcessor:
    @staticmethod
    def save_file(df: DataFrame, folder_path: str) -> bool:
        try:
            if not df.rdd.isEmpty():
                df.write.mode('overwrite').json(folder_path)
                print(f"✅ Data written to {folder_path}")
                return True
            else:
                print("⚠️ Skipped empty DataFrame")
                return False
        except Exception as e:
            raise RuntimeError(f"❌ Error saving JSON: {e}")

    @staticmethod
    def save_json_file(temp_path: str, redis_path: str, dt: str, filename: str):
        file_info_list = dbutils.fs.ls(temp_path)
        json_files = [f for f in file_info_list if f.name.endswith('.json') and f.size > 10]
        # formatted_date=dt.strftime("%Y%m%dT%H%M%S")
        if not json_files:
            print("⚠️ No valid JSON files to move.")
            return

        dest_path = f"{redis_path}/{dt}/"
        # new_file_path = f"{dest_path}{filename}_formatted_date'.json"
        # cunnt=1
        for file_info in json_files:
            new_file_path = f"{dest_path}{filename}.json"
            dbutils.fs.mv(file_info.path, new_file_path)
            # cunnt += 1
            print(f"✅ Moved to: {new_file_path}")

    @staticmethod
    def cleanup_temp(temp_path: str):
        try:
            dbutils.fs.rm(temp_path, recurse=True)
            print(f"🧹 Cleaned temp path: {temp_path}")
        except Exception as e:
            print(f"⚠️ Failed to clean temp path: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check If Redis Has Any Data

# COMMAND ----------

key_cunnt = redis_client.dbsize()  # Cunnt total keys in the database
print(f"Total keys in Redis: {key_cunnt}")


# COMMAND ----------

import redis
import json
from pyspark.sql.functions import substring, col, lit
from datetime import datetime
import pandas as pd

keys = redis_client.keys("*")

for key in keys:
    value = redis_client.hgetall(key)
    print(key,value)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing into Titan data lake

# COMMAND ----------

# %python
# import redis
# import json
# from pyspark.sql.functions import substring, col, lit
# from datetime import datetime
# import pandas as pd

# # Ensure metadata file exists
# metadata_path = "dbfs:/mnt/dummydatalake/DataSunrce/LandingZone/dummyUN/Redis/metadata/redis_metadata.json"

# # Read metadata
# lastUpdatedTime = MetadataHandler.read_metadata(metadata_path, default_metadata=None, key=metadata_key)

# # Get all keys from Redis
# keys = redis_client.keys("*")
# cunnt = 1
# DateTimeUpdatedUtc = []
# data_list = []

# for key in keys:
#     value = redis_client.hgetall(key)
#     dict = json.loads(value['data'])
#     keyAttr = key.split(':')[1]

#     # Checking for the latest updated file time
#     try:
#         if dict['Value']['DateTimeUpdatedUtc'] > lastUpdatedTime['DateTimeUpdatedUtc']:
#             todate = dict['Value']['DateTimeUpdatedUtc'].split('T')[0]
#             DateTimeUpdatedUtc.append(dict['Value']['DateTimeUpdatedUtc'])
#             dt = datetime.strptime(dict['Value']['DateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
#             formatted_date = dt.strftime("%Y%m%dT%H%M%S")
#             df = spark.createDataFrame([dict['Value']])
#             PI = dict['Value']['PI']
#             SRNumber = dict['Value']['SRNumber']
#             ActivityNumber = dict['Value']['ActivityNumber']
#             print(PI, SRNumber, ActivityNumber)
#             df = df.withColumn("RedisKey", lit(key.split(':')[1]))
#             df_reordered = df.select("Response", "RedisKey", "PI", "SRNumber", "ActivityNumber", "CreatedByOucu", "DateTimeCreatedUtc", "DateTimeUpdatedUtc", "Links", "Feedbacks")
#             DataProcessor.save_file(df_reordered, f'{temp_path}/{keyAttr}_{cunnt}.json')
#             DataProcessor.save_json_file(temp_path, redis_path, todate, f'dummyUN_redis_{formatted_date}')
#             cunnt += 1
#     except Exception as e:
#         raise RuntimeError(f"An error occurred while processing the data: {e}")

# # Update metadata file DataSunrce/LandingZone/dummyUN/Redis/metadata/redis_metadata.json
# if len(DateTimeUpdatedUtc) != 0:
#     MetadataHandler.update_metadata(metadata_path, metadata_key, max(DateTimeUpdatedUtc))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing into Titan data lake if data is in camel case in Redis webapp

# COMMAND ----------

# import redis
# import json
# from pyspark.sql.functions import substring, col, lit
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType
# from datetime import datetime
# import pandas as pd

# # Define schema with nested structures
# schema = StructType([
#     StructField("response", StringType(), True),
#     StructField("pi", StringType(), True),
#     StructField("srNumber", StringType(), True),
#     StructField("activityNumber", StringType(), True),
#     StructField("createdByOucu", StringType(), True),
#     StructField("dateTimeCreatedUtc", TimestampType(), True),
#     StructField("dateTimeUpdatedUtc", TimestampType(), True),
#     StructField("links", ArrayType(MapType(StringType(), StringType())), True),
#     StructField("feedbacks", ArrayType(MapType(StringType(), StringType())), True)
# ])

# # Ensure metadata file exists
# metadata_path = "dbfs:/mnt/dummydatalake/DataSunrce/LandingZone/dummyUN/Redis/metadata/redis_metadata.json"

# # Read metadata
# lastUpdatedTime = MetadataHandler.read_metadata(metadata_path, default_metadata=None, key=metadata_key)

# # Get all keys from Redis
# keys = redis_client.keys("*")
# cunnt = 1
# DateTimeUpdatedUtc = []
# data_list = []

# for key in keys:
#     value = redis_client.hgetall(key)
#     dict = json.loads(value['data'])
#     keyAttr = key.split(':')[1]

#     # Checking for the latest updated file time
#     try:
#         if dict['Value']['DateTimeUpdatedUtc'] > lastUpdatedTime['DateTimeUpdatedUtc']:
#             todate = dict['Value']['DateTimeUpdatedUtc'].split('T')[0]
#             DateTimeUpdatedUtc.append(dict['Value']['DateTimeUpdatedUtc'])
#             dt = datetime.strptime(dict['Value']['DateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
#             formatted_date = dt.strftime("%Y%m%dT%H%M%S")
#             df = spark.createDataFrame([dict['Value']])
#             PI = dict['Value']['PI']
#             SRNumber = dict['Value']['SRNumber']
#             ActivityNumber = dict['Value']['ActivityNumber']
#             print(PI, SRNumber, ActivityNumber)
#             df = df.withColumn("RedisKey", lit(key.split(':')[1]))
#             df_reordered = df.select("Response", "RedisKey", "PI", "SRNumber", "ActivityNumber", "CreatedByOucu", "DateTimeCreatedUtc", "DateTimeUpdatedUtc", "Links", "Feedbacks")
#             DataProcessor.save_file(df_reordered, f'{temp_path}/{keyAttr}_{cunnt}.json')
#             DataProcessor.save_json_file(temp_path, redis_path, todate, f'dummyUN_redis_{formatted_date}')
#             cunnt += 1
#     except Exception as e:
#         try:
#             if dict['value']['dateTimeUpdatedUtc'] > lastUpdatedTime['DateTimeUpdatedUtc']:
#                 todate = dict['value']['dateTimeUpdatedUtc'].split('T')[0]
#                 DateTimeUpdatedUtc.append(dict['value']['dateTimeUpdatedUtc'])
#                 dt = datetime.strptime(dict['value']['dateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
#                 formatted_date = dt.strftime("%Y%m%dT%H%M%S")
#                 dict['value']['dateTimeCreatedUtc'] = datetime.strptime(dict['value']['dateTimeCreatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
#                 dict['value']['dateTimeUpdatedUtc'] = datetime.strptime(dict['value']['dateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
#                 df = spark.createDataFrame([dict['value']], schema=schema)
#                 pi = dict['value']['pi']
#                 srNumber = dict['value']['srNumber']
#                 activityNumber = dict['value']['activityNumber']
#                 df = df.withColumn("redisKey", lit(key.split(':')[1]))
#                 df_reordered = df.select("Response", "RedisKey", "PI", "SRNumber", "ActivityNumber", "CreatedByOucu", "DateTimeCreatedUtc", "DateTimeUpdatedUtc", "Links", "Feedbacks")
#                 DataProcessor.save_file(df_reordered, f'{temp_path}/{keyAttr}_{cunnt}.json')
#                 DataProcessor.save_json_file(temp_path, redis_path, todate, f'dummyUN_redis_{formatted_date}')
#                 cunnt += 1
#         except Exception as e:
#             raise RuntimeError(f"An error occurred while processing the data: {e}")

# dbutils.fs.ls(temp_path)
# # Cleanup temp file 
# DataProcessor.cleanup_temp_file(temp_path)
# # Update metadata file DataSunrce/LandingZone/dummyUN/Redis/metadata/redis_metadata.json
# if len(DateTimeUpdatedUtc) != 0:
#     MetadataHandler.update_metadata(metadata_path, metadata_key, max(DateTimeUpdatedUtc))

# COMMAND ----------

import redis
import json
from pyspark.sql.functions import substring, col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType
from datetime import datetime
import pandas as pd

# Define schema with nested structures
schema = StructType([
    StructField("response", StringType(), True),
    StructField("pi", StringType(), True),
    StructField("srNumber", StringType(), True),
    StructField("activityNumber", StringType(), True),
    StructField("createdByOucu", StringType(), True),
    StructField("dateTimeCreatedUtc", TimestampType(), True),
    StructField("dateTimeUpdatedUtc", TimestampType(), True),
    StructField("links", ArrayType(MapType(StringType(), StringType())), True),
    StructField("feedbacks", ArrayType(MapType(StringType(), StringType())), True)
])

# Ensure metadata file exists
metadata_path = "dbfs:/mnt/dummydatalake/DataSunrce/LandingZone/dummyUN/Redis/metadata/redis_metadata.json"

# Read metadata
lastUpdatedTime = MetadataHandler.read_metadata(metadata_path, default_metadata=None, key=metadata_key)
print(lastUpdatedTime)
# Get all keys from Redis
keys = redis_client.keys("*")
cunnt = 1
DateTimeUpdatedUtc = []
data_list = []

for key in keys:
    value = redis_client.hgetall(key)
    dict = json.loads(value['data'])
    keyAttr = key.split(':')[1]
    try:
        dateTimeUpdatedUtc_str = dict['value']['dateTimeUpdatedUtc'][:19]
        dateTimeUpdatedUtc_format = "%Y-%m-%dT%H:%M:%S"
        if datetime.strptime(dateTimeUpdatedUtc_str, dateTimeUpdatedUtc_format) > datetime.strptime(lastUpdatedTime['DateTimeUpdatedUtc'], '%Y-%m-%d %H:%M:%S'):
            print(datetime.strptime(dateTimeUpdatedUtc_str, dateTimeUpdatedUtc_format))
            print(datetime.strptime(lastUpdatedTime['DateTimeUpdatedUtc'], '%Y-%m-%d %H:%M:%S'))
            todate = dict['value']['dateTimeUpdatedUtc'].split('T')[0]
            
            dt = datetime.strptime(dict['value']['dateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
            formatted_date = dt.strftime("%Y%m%dT%H%M%S")
            dict['value']['dateTimeCreatedUtc'] = datetime.strptime(dict['value']['dateTimeCreatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
            dict['value']['dateTimeUpdatedUtc'] = datetime.strptime(dict['value']['dateTimeUpdatedUtc'][:19], "%Y-%m-%dT%H:%M:%S")
            df = spark.createDataFrame([dict['value']], schema=schema)
            pi = dict['value']['pi']
            srNumber = dict['value']['srNumber']
            activityNumber = dict['value']['activityNumber']
            df = df.withColumn("redisKey", lit(key.split(':')[1]))
            df_reordered = df.select("Response", "RedisKey", "PI", "SRNumber", "ActivityNumber", "CreatedByOucu", "DateTimeCreatedUtc", "DateTimeUpdatedUtc", "Links", "Feedbacks")
            DateTimeUpdatedUtc.append(datetime.strptime(dateTimeUpdatedUtc_str, dateTimeUpdatedUtc_format))
            DataProcessor.save_file(df_reordered, f'{temp_path}')
            # DataProcessor.save_file(df_reordered, f'{temp_path}')
            
            DataProcessor.save_json_file(temp_path, redis_path, todate, f'dummyUN_redis_{formatted_date}')
            cunnt += 1
            
    except Exception as e:
        raise RuntimeError(f"An error occurred while processing the data: {e}")


# Cleaning temp folder and update metadata
if DateTimeUpdatedUtc:
    DataProcessor.cleanup_temp(temp_path)
    MetadataHandler.update_metadata(metadata_path, metadata_key, max(DateTimeUpdatedUtc))
