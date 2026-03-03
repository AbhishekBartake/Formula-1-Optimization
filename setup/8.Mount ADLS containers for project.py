# Databricks notebook source
# MAGIC %md
# MAGIC #### Access to ADLS2 using Shared Access token 
# MAGIC 1. get client_id, tenant_id and client_secret from key vault 
# MAGIC 2. call the file system utility mount to mount the storage 
# MAGIC 3. set spark config with app/client id, directory tenant id & secret 
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data lake 

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    client_id = dbutils.secrets.get(scope = "f1dl2", key = "client_id")
    tenant_id = dbutils.secrets.get(scope = "f1dl2", key = "tenant_id")
    client_secret = dbutils.secrets.get(scope = "f1dl2", key = "client_secret")

    #set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #mount the containers
    dbutils.fs.mount(source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net", 
                 mount_point=f"/mnt/f1dl2/{container_name}", 
                 extra_configs=configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

client_id
tenant_id
client_secret
#these you'll find in your Azure portal under Entra ID 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source="abfss://demo@f1dl2.dfs.core.windows.net", 
                 mount_point="/mnt/demo", 
                 extra_configs=configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/f1dl2/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dl2/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dl2.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl2.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

