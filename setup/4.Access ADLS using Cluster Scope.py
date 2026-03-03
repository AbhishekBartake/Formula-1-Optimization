# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@f1dl2.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dl2.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

