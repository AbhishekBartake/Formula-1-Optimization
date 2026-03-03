# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-03-02")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results")\
    .filter(f"file_date='{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum,desc,count,when,col,rank

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results").filter(col("race_year").isin(race_year_list))
    

# COMMAND ----------

driver_standings_df = race_results_df\
    .groupBy("race_year","driver_id","driver_name","driver_nationality","team_name")\
    .agg(sum("points").alias("total_points"),
        count(when(col("position")==1.0,True)).alias("wins"))   

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc,row_number
   
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df =  driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))
deduped_df = final_df.withColumn("row_num", row_number().over(driver_rank_spec))\
                     .filter("row_num = 1")\
                     .drop("row_num")

# COMMAND ----------

deduped_df.createOrReplaceTempView("final_drivers_view")

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_year = src.race_year"
merge_delta_data(final_df,"f1_presentation","driver_standings","final_drivers_view",merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings

# COMMAND ----------

# MAGIC %sql 
# MAGIC select driver_name,count(1)
# MAGIC from f1_presentation.driver_standings 
# MAGIC group by driver_name
# MAGIC having count(1)>1 
# MAGIC order by driver_name desc