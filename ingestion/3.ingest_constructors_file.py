# Databricks notebook source
# MAGIC %md
# MAGIC ### Read file using dataframe reader (we'll be using DDL schemas)
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

dbutils.widgets.text("p_data_source","ERGAST API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2024-02-24')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_df = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_drop_df = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new column 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_drop_df)\
                                            .withColumnRenamed("constructorId","constructor_id")\
                                            .withColumnRenamed("constructorRef","constructor_ref")\
                                            .withColumn("data_source",lit(v_data_source))\
                                            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to parquet file
# MAGIC

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

constructors_final_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/constructors")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")