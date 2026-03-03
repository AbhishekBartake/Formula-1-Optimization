# Databricks notebook source
# MAGIC %md
# MAGIC #### Access to ADLS2 using Shared Access token 
# MAGIC 1. Set the spark config to SAS token 
# MAGIC 2. List files from the demo container
# MAGIC 3. Read data from the circuits.csv file

# COMMAND ----------

formula1dl3_account_key = dbutils.secrets.get("f1dl3-scope", "SAS-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dl3.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1dl3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1dl3.dfs.core.windows.net", formula1dl3_account_key)

# COMMAND ----------

dbutils.secrets.list("f1dl3-scope")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dl3.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl3.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

