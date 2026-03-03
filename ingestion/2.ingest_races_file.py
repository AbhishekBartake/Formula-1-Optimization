# Databricks notebook source
# MAGIC %md
# MAGIC ###Read the file using DataframeReader API
# MAGIC

# COMMAND ----------

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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType 

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True),
                                  StructField("fp1_date",DateType(),True),
                                  StructField("fp1_time",StringType(),True),
                                  StructField("fp2_date",DateType(),True),
                                  StructField("fp2_time",StringType(),True),
                                  StructField("fp3_date",DateType(),True),
                                  StructField("fp3_time",StringType(),True),
                                  StructField("quali_date",DateType(),True),
                                  StructField("quali_time",StringType(),True),
                                  StructField("sprint_date",DateType(),True),
                                  StructField("sprint_time",StringType(),True),
                                  ])

# COMMAND ----------

races_df = spark.read \
    .option("header",True)\
    .schema(races_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adding New columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit,col

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df) \
                          .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                          .withColumn("data_source",lit(v_data_source))\
                          .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop URL column and rename each columnn according to the convention

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias('race_id'),col("year").alias('race_year'),col("round"),col("circuitId").alias('circuit_id'),col("name"),col("date"),col("time"),col("fp1_date"),col("fp1_time"),col("fp2_date"),col("fp2_time"),col("fp3_date"),col("fp3_time"),col("quali_date"),col("quali_time"),col("sprint_date"),col("sprint_time"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###write Data to the Processed container

# COMMAND ----------

races_selected_df.write.format("delta").partitionBy('race_year').mode("overwrite").saveAsTable("f1_processed.races")

# COMMAND ----------

races_selected_df.write.format("delta").mode("overwrite").partitionBy('race_year').save(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("success")