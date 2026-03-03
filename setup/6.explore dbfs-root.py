# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore dbfs root 
# MAGIC 1. List all the folders in the dbfs root 
# MAGIC 2. Interact with dbfs file browser 
# MAGIC 3. upload file to dbfs root 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))