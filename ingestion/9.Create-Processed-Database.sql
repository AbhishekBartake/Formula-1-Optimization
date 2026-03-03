-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

CREATE DATABASE if not exists f1_processed
MANAGED LOCATION "abfss://processed@f1dl3.dfs.core.windows.net/"

-- COMMAND ----------

DESCRIBE DATABASE f1_processed

-- COMMAND ----------

