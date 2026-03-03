# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("race_results_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM race_results_view
# MAGIC WHERE race_year=2020

# COMMAND ----------

p_race_year = 2020


# COMMAND ----------

race_data_df = spark.sql(f"SELECT * FROM race_results_view WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_data_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC global_temp.gv_race_results_view