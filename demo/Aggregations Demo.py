# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name=='Max Verstappen'").select(sum("points")).show()

# COMMAND ----------

demo_df.groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points")\
    ,countDistinct("race_name").alias("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####WIndow Functions 

# COMMAND ----------

demo_window = race_results.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_window_df=demo_window.groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points")\
    ,countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_window_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driver_rank = Window.partitionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

demo_window_df.withColumn("rank",rank().over(driver_rank)).show(100)

# COMMAND ----------

