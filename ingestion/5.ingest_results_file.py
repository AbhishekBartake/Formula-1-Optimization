# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","ERGAST API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-03-02")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# COMMAND ----------

results_schema =StructType(fields = [StructField("resultId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",FloatType(),True),
                                     StructField("grid",IntegerType(),True),
                                     StructField("position",FloatType(),True),
                                     StructField("positionText",StringType(),True),
                                     StructField("positionOrder",IntegerType(),True),
                                     StructField("points",FloatType(),True),
                                     StructField("laps",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",FloatType(),True),
                                     StructField("fastestLap",FloatType(),True),
                                     StructField("rank",FloatType(),True),
                                     StructField("fastestLapTime",StringType(),True),
                                     StructField("fastestLapSpeed",StringType(),True),
                                     StructField("statusId",IntegerType(),True)
])

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_df).withColumnRenamed("resultId","result_id")\
                               .withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                               .withColumnRenamed("constructorId","constructor_id")\
                               .withColumnRenamed("positionText","position_text")\
                               .withColumnRenamed("positionOrder","position_order")\
                               .withColumnRenamed("fastestLap","fastest_lap")\
                               .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                               .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                               .withColumn("data_source", lit(v_data_source))\
                               .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

results_final_df=results_renamed_df.drop(col('statusId'))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

results_deduped_df.createOrReplaceTempView("results_final_view")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id=src.race_id"
merge_delta_data(results_deduped_df,"f1_processed","results","results_final_view",merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id,count(1) from f1_processed.results group by race_id order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id,driver_id,count(1)
# MAGIC from f1_processed.results 
# MAGIC group by race_id,driver_id 
# MAGIC having count(1)>1 
# MAGIC order by race_id,driver_id desc