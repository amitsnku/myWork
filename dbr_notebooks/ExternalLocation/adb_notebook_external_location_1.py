# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
ws=WorkspaceClient()
ws_details=ws.metastores.current()
metastore_id=ws_details.metastore_id
workspace_id=ws_details.workspace_id
subscription_id=dbutils.widgets.get("adfplparam_subscription_id_1")
resourceGroup_name=dbutils.widgets.get("adfplparam_rg_name_1")
accessConnector_name=dbutils.widgets.get("adfglparam_accessConnector_name_1")
env=dbutils.widgets.get("adfplparam_scale_env_1")
stg_cred_name=f'cred-databricks-ac-outitan{env}adlsgen2'
user_group=dbutils.widgets.get("adfglparam_user_group_name_1")

host=ws.config.host

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assigning  metastore to workspace 

# COMMAND ----------

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
ws_details = ws.metastores.current()

ws.metastores.assign(
    metastore_id=ws_details.metastore_id,
    workspace_id=ws_details.workspace_id,
    default_catalog_name=""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creat storage credntial
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
# from databricks.sdk.service import Catalog 
# dir(WorkspaceClient)
w=WorkspaceClient()
# help(w.storage_credentials.create)
ls=[stgnm.name for stgnm in w.storage_credentials.list()]
access_connector_rsrc_id=f'/subscriptions/{subscription_id}/resourceGroups/{resourceGroup_name}/providers/Microsoft.Databricks/accessConnectors/{accessConnector_name}'
if stg_cred_name not in ls:
    credential=w.storage_credentials.create(
      name=stg_cred_name,
      azure_managed_identity=catalog.AzureManagedIdentityResponse(
        access_connector_id=access_connector_rsrc_id
      ),
      comment=f'created using azure managed identity : {accessConnector_name}'

    )



spark.sql(f"GRANT MANAGE ON STORAGE CREDENTIAL `{stg_cred_name}` TO DataTechAdmin ")
spark.sql(f"GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL `{stg_cred_name}` TO DataTechAdmin ")




# COMMAND ----------

# MAGIC %md
# MAGIC #### For data source (read only)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# Listing existing external locations

ls = [ext_loc_nm.name for ext_loc_nm in w.external_locations.list()]

# For External location against datasource path

ext_loc_name = f'ext-loc-gen2stg-{env}-datasource'

# Create external location against datasource path if not exist 

if ext_loc_name not in ls:
    external_location = w.external_locations.create(
        name=ext_loc_name,
        credential_name=stg_cred_name,
        url=f'abfss://outitan{env}adls@outitan{env}adlsgen2.dfs.core.windows.net/DataSource/',
        read_only=False,
        comment=f'created for DataSource using sdk based on managed identity : {accessConnector_name}'
    )


# GRANT READ FILES permission to group 'ou-titan-dev-datascientists-grp'

# user_group = 'ou-titan-dev-datascientists-grp'


spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO `{user_group}`")
spark.sql(f"GRANT MANAGE ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin ")
spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")
spark.sql(f"GRANT WRITE FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")

# COMMAND ----------

# MAGIC %md
# MAGIC #### For data service (read only) 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# Listing existing external locations

ls = [ext_loc_nm.name for ext_loc_nm in w.external_locations.list()]


# For External location against datasource path

ext_loc_name = f'ext-loc-gen2stg-{env}-dataservice'

# Create external location against datasource path if not exist 

if ext_loc_name not in ls:
    external_location = w.external_locations.create(
        name=ext_loc_name,
        credential_name=stg_cred_name,
        url=f'abfss://outitan{env}adls@outitan{env}adlsgen2.dfs.core.windows.net/DataService/',
        read_only=False,
        comment=f'created for DataService using sdk based on managed identity : {accessConnector_name}'
    )


# GRANT READ FILES permission to group 'ou-titan-dev-datascientists-grp'

# user_group = 'ou-titan-dev-datascientists-grp'

spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO `{user_group}`")
spark.sql(f"GRANT MANAGE ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin ")
spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")
spark.sql(f"GRANT WRITE FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### For data sceince (read and write) 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# Listing existing external locations

ls = [ext_loc_nm.name for ext_loc_nm in w.external_locations.list()]

# For External location against datasource path

ext_loc_name = f'ext-loc-gen2stg-{env}-dataworks-teams-datascience'

# Create external location against datasource path if not exist 

if ext_loc_name not in ls:
    external_location = w.external_locations.create(
        name=ext_loc_name,
        credential_name=stg_cred_name,
        url=f'abfss://unitan{env}adls@oudummy{env}adlsgen2.dfs.core.windows.net/DataWork/Teams/DataScience/',
        read_only=False,
        comment=f'created for DataScience using sdk based on managed identity : {accessConnector_name}'
        
    )


# GRANT READ FILES permission to group 'ou-dummy-dev-datascientists-grp'

# user_group = 'ou-dummy-dev-datascientists-grp'

spark.sql(f"GRANT READ FILES,WRITE FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO `{user_group}`")
spark.sql(f"GRANT MANAGE ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin ")
spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")
spark.sql(f"GRANT WRITE FILES ON EXTERNAL LOCATION `{ext_loc_name}` TO DataTechAdmin")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create Catalog

# COMMAND ----------

# %python
# external_loc_path='abfss://oudummydevadls@oudummydevadlsgen2.dfs.core.windows.net/DataSource'
# # external_loc_path='abfss://oudummydevadls@oudummydevadlsgen2.dfs.core.windows.net/DataService'
# # external_loc_path='abfss://oudummydevadls@oudummydevadlsgen2.dfs.core.windows.net/DataWork/Teams/DataScience'
# display(dbutils.fs.ls(external_loc_path))