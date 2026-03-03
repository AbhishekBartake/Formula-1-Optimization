# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df    

# COMMAND ----------

# def rearrange_partition(input_df,partition_column):

#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     output_df=input_df.select(column_list)
#     return output_df

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = rearrange_partition(input_df, partition_column)

#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

#     def table_exists(db_name, table_name):
#         try:
#             spark.table(f"{db_name}.{table_name}")
#             return True
#         except AnalysisException:
#             return False

#     if table_exists(db_name, table_name):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite") \
#             .partitionBy(partition_column) \
#             .format("delta") \
#             .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# from pyspark.sql.utils import AnalysisException

# def merge_delta_data(input_df, db_name, table_name, view_name, merge_condition, partition_column):
#     spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

#     def table_exists(db_name, table_name):
#         try:
#             spark.catalog.tableExists(f"{db_name}.{table_name}")
#             return True
#         except AnalysisException:
#             return False

#     if table_exists(db_name, table_name):
#         spark.sql(f"""
#             MERGE INTO {db_name}.{table_name} AS tgt
#             USING {view_name} AS src
#             ON {merge_condition}
#             WHEN MATCHED THEN UPDATE SET *
#             WHEN NOT MATCHED THEN INSERT *
#         """)
#     else:
#         input_df.write.mode("overwrite") \
#             .partitionBy(partition_column) \
#             .format("delta") \
#             .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, view_name, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        spark.sql(f"""
            MERGE INTO {db_name}.{table_name} AS tgt
            USING {view_name} AS src
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        input_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name)\
                          .distinct()\
                          .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list