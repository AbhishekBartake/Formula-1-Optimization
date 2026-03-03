# Databricks notebook source
# MAGIC %md
# MAGIC ###inner Join 

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id<70")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter(col("race_year") == 2021)\
    .withColumnRenamed("name","races_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

join_df = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(join_df)

# COMMAND ----------

join_df.select("circuit_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###left outer join

# COMMAND ----------

join_df = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"left")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###right outer join
# MAGIC
# MAGIC

# COMMAND ----------

join_df = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"right")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(join_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###full outer join

# COMMAND ----------

join_df = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"full")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.races_name,races_df.round)

# COMMAND ----------

display(join_df)

# COMMAND ----------

join_df = circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"anti")

# COMMAND ----------

display(join_df)

# COMMAND ----------

join_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(join_df)

# COMMAND ----------

