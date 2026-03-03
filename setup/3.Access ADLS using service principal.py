# Databricks notebook source
# MAGIC %md
# MAGIC #### Access to ADLS2 using Shared Access token 
# MAGIC 1. Register Azure AD application/service principal
# MAGIC 2. Generate a secret for the application
# MAGIC 3. set spark config with app/client id, directory tenant id & secret 
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data lake 

# COMMAND ----------

# client_id="{Client_id}"
# tenant_id="{tenant_id}"
# client_secret="{client_secret}"
#these you'll find in your Azure portal under Entra ID 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dl3.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1dl3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1dl3.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1dl3.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1dl3.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dl3.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl3.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

