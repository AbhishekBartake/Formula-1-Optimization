# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-02-24")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import count,col,when,desc,sum,rank

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df\
    .groupBy("race_year","team_name")\
    .agg(sum("points").alias("total_points"),
        count(when(col("position") == 1.0, True)).alias("wins")
        )

# COMMAND ----------

from pyspark.sql.window import Window

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))  

# COMMAND ----------

final_df.createOrReplaceTempView("constructor_standings_view")


# COMMAND ----------

merge_condition = "tgt.team_name = src.team_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings',"constructor_standings_view", merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_presentation.constructor_standings where race_year =2021