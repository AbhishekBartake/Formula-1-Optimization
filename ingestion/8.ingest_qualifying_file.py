# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","ERGAST API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-02-24")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

qualify_schema=StructType(fields=[
                                    StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualify_df=spark.read\
    .schema(qualify_schema)\
    .option("multiLine", True)\
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

qualify_select_df=add_ingestion_date(qualify_df)\
    .withColumnRenamed("qualifyId", "qualifying_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# overwrite_partition(qualify_select_df,'f1_processed','qualifying','race_id')


# COMMAND ----------

qualify_select_df.createOrReplaceTempView("qualify_tmp")

# COMMAND ----------

merge_condition = "tgt.qualifying_id=src.qualifying_id and tgt.race_id=src.race_id"
merge_delta_data(qualify_select_df,"f1_processed","qualifying","qualify_tmp",merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying

# COMMAND ----------

dbutils.notebook.exit("success")