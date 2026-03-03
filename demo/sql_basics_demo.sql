-- Databricks notebook source
-- MAGIC %md
-- MAGIC SQL simple Functions 

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

select *,concat(driver_ref,'-', code) AS new_driver_ref from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Aggregate functions

-- COMMAND ----------

select nationality, count(*) from drivers group by nationality having count(*) > 100 order by nationality

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC window functions

-- COMMAND ----------

select nationality,name,dob,RANK() over (partition by nationality order by dob desc) as age_rank
from drivers
order by nationality,age_rank

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC JOINS

-- COMMAND ----------

