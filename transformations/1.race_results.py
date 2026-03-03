# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-02-24")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.table("f1_processed.drivers")\
    .withColumnRenamed("driver_id", "drivers_table_driver_id")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.table("f1_processed.constructors")\
    .withColumnRenamed("name","team_name")

# COMMAND ----------

races_df = spark.table("f1_processed.races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circuits_df =spark.table("f1_processed.circuits")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

results_df = spark.table("f1_processed.results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("time", "race_time")\
    .withColumnRenamed("race_id", "results_race_id")\
    .withColumnRenamed("file_date", "result_file_date")


# COMMAND ----------

# MAGIC %md
# MAGIC ###join circuits to races

# COMMAND ----------

circuits_races_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner")\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join to other dataframes

# COMMAND ----------

race_results_df = results_df.join(circuits_races_df, results_df.results_race_id == circuits_races_df.race_id)\
                            .join(drivers_df, results_df.driver_id == drivers_df.drivers_table_driver_id)\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,desc

# COMMAND ----------

final_df = race_results_df.select("race_id",results_df.driver_id.alias("driver_id"),"race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team_name","grid","fastest_lap","race_time","points","position","result_file_date")\
                            .withColumn("created_data",current_timestamp())\
                            .withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceTempView("final_view")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_id=src.driver_id"
merge_delta_data(final_df,"f1_presentation","race_results","final_view",merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id,race_year from f1_presentation.race_results group by race_id,race_year order by race_year desc