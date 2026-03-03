# Databricks notebook source


# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.f1dl3.dfs.core.windows.net",
#     "{Access_key}")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dl3.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl3.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

