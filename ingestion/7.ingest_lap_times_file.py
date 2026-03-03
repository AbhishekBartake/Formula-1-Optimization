# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","ERGAST API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-02-24")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

lap_times_schema=StructType(fields=[
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df=spark.read\
    .schema(lap_times_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_final_df=add_ingestion_date(lap_times_df)\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

lap_times_final_df.createOrReplaceTempView("lap_times_tmp")

# COMMAND ----------

merge_condition = "tgt.race_id=src.race_id and tgt.driver_id=src.driver_id and tgt.lap=src.lap"
merge_delta_data(lap_times_final_df,"f1_processed","lap_times","lap_times_tmp",merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.lap_times

# COMMAND ----------

dbutils.notebook.exit("success")