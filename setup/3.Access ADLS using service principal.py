# Databricks notebook source
# MAGIC %md
# MAGIC #### Access to ADLS2 using Shared Access token 
# MAGIC 1. Register Azure AD application/service principal
# MAGIC 2. Generate a secret for the application
# MAGIC 3. set spark config with app/client id, directory tenant id & secret 
# MAGIC 4. Assign role 'Storage Blob Data Contributor' to the Data lake 

# COMMAND ----------

client_id="dc6c5244-24aa-4fb3-8a92-a5c0a95c95c6"
tenant_id="ee7be4de-db7f-4948-bde1-f41f342c050d"
client_secret="Sz38Q~yhvkHyATQoXmxRlA5LlRAKR~IlJ28zZaZ4"
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

