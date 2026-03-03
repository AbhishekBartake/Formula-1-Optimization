# Databricks notebook source
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType 

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename",StringType(),True)
                                   ,StructField("surname",StringType(),True)
                                   ])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("code",StringType(),True),
    StructField("name",name_schema),
    StructField("dob",DateType(),True),
    StructField("nationality",StringType(),True),
    StructField("url",StringType(),True)    
])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add ingestion date column and rename the columns 
# MAGIC
# MAGIC   

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

drivers_rename_df = add_ingestion_date(drivers_df)\
                              .withColumnRenamed("driverId","driver_id")\
                              .withColumnRenamed("driverRef","driver_ref")\
                              .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
                              .withColumn("data_source",lit(v_data_source))\
                              .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_rename_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

drivers_final_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/drivers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("success")