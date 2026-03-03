-- Databricks notebook source
-- MAGIC %md
-- MAGIC this is how managed table is created 

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("`uc-metastore`.demo.race_results_python")

-- COMMAND ----------

USE `uc-metastore`.demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM race_results_python WHERE race_year = 2024

-- COMMAND ----------

CREATE TABLE race_results_sql 
as 
SELECT * FROM race_results_python
WHERE race_year =2024

-- COMMAND ----------

select * from race_results_sql

-- COMMAND ----------

desc extended race_results_sql

-- COMMAND ----------

drop table race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC this is how we write external tables 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results").mode("overwrite").saveAsTable("`uc-metastore`.demo.race_results_python_ext")

-- COMMAND ----------

use `uc-metastore`.demo;
desc extended race_results_python_ext

-- COMMAND ----------

show tables in `uc-metastore`.demo

-- COMMAND ----------

