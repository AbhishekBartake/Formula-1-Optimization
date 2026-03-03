# Databricks notebook source


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1dl3.dfs.core.windows.net",
    "VBpbTLypK6Jkaey/6cxG4Fgyg50PaFtmZaREUG03UTUrZAEI6TA+6+TE/kbifW+z7t8F3P05CXKM+AStEId5Tg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dl3.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl3.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

