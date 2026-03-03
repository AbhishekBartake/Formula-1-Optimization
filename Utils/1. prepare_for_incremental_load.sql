-- Databricks notebook source
-- drop database if exists f1_processed cascade

-- COMMAND ----------

-- create database if not exists f1_processed 
-- managed location "abfss://processed@f1dl3.dfs.core.windows.net/"

-- COMMAND ----------

drop database if exists f1_presentation cascade

-- COMMAND ----------

create database if not exists f1_presentation 
managed location "abfss://presentation@f1dl3.dfs.core.windows.net/"