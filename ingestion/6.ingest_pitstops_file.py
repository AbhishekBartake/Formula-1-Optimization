# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","ERGAST API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2024-02-24')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

pit_stops_schema=StructType(fields=[
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df=spark.read\
    .schema(pit_stops_schema)\
    .option("multiLine", True)\
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pit_stops_final_df=add_ingestion_date(pit_stops_df)\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# overwrite_partition(pit_stops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

pit_stops_final_df.createOrReplaceTempView("pit_stops_tmp")

# COMMAND ----------

merge_condition = "tgt.race_id=src.race_id and tgt.driver_id=src.driver_id and tgt.stop=src.stop"
merge_delta_data(pit_stops_final_df,"f1_processed","pit_stops","pit_stops_tmp",merge_condition,"race_id")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id,count(1) from f1_processed.pit_stops group by race_id order by race_id desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.pit_stops