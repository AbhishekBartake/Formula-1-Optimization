# Databricks notebook source
# MAGIC %run 
# MAGIC "../Includes/configuration"

# COMMAND ----------

# %sql 
# create database if not exists f1_demo 
# managed location "abfss://demo@f1dl3.dfs.core.windows.net/"

# COMMAND ----------

results_df = spark.read\
.option("inferSchema",True)\
.json(f"{raw_folder_path}/2024-03-02/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location "abfss://demo@f1dl3.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df= spark.read.format("delta").load(f"{demo_folder_path}/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned ")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC Update delta table 
# MAGIC Delete from delta table  

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC   set points = 11- position 
# MAGIC where position <=10 

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "f1_demo.results_managed")
deltaTable.update("position<=10",{"points","21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_managed
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge <br> 1.Insert any new records being received <br> 2.Update any existing records for which new data has been received <br> 3.Apply delete as well

# COMMAND ----------

drivers_df_day1 = spark.read\
.option("inferSchema",True)\
.json(f"{raw_folder_path}/2024-03-02/drivers.json")\
.filter("driverId <= 10")\
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_df_day1)

# COMMAND ----------

from pyspark.sql.functions import upper 

drivers_df_day2 = spark.read\
.option("inferSchema",True)\
.json(f"{raw_folder_path}/2024-03-02/drivers.json")\
.filter("driverId between 6 and 15")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_df_day2)

# COMMAND ----------

drivers_df_day3 = spark.read\
.option("inferSchema",True)\
.json(f"{raw_folder_path}/2024-03-02/drivers.json")\
.filter("driverId between 1 and 5 or driverId between 16 and 20")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_df_day1.createOrReplaceTempView("drivers_day1")
drivers_df_day2.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists f1_demo.drivers_merged(
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using delta 

# COMMAND ----------

# MAGIC %md 
# MAGIC Day 1 

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into f1_demo.drivers_merged tgt
# MAGIC using drivers_day1 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC when not matched
# MAGIC   then insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merged

# COMMAND ----------

# MAGIC %md 
# MAGIC Day 2 

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into f1_demo.drivers_merged tgt
# MAGIC using drivers_day2 upd
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC when not matched
# MAGIC   then insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merged

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.drivers_merged 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merged timestamp as of "2025-06-21T07:27:52.000+00:00"

# COMMAND ----------

# MAGIC %sql 
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merged retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merged where driverId=1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merged

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into f1_demo.drivers_merged tgt
# MAGIC using f1_demo.drivers_merged version as of 3 src
# MAGIC   on (tgt.driverId = src.driverId)
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.drivers_merged

# COMMAND ----------

# MAGIC %sql 
# MAGIC convert to delta f1_demo.drivers_merged

# COMMAND ----------

#convert to delta with file system 
# %sql 
# convert to delta parquet.'<path to the file>'