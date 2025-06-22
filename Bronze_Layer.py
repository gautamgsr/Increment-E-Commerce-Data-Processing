# Databricks notebook source
dbutils.widgets.text('file_name','')
file_name = dbutils.widgets.get('file_name')


# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation",f"abfss://raw@devdolphins.dfs.core.windows.net/ETE/checkpoints_{file_name}")\
    .load(f'abfss://source@devdolphins.dfs.core.windows.net/ETE/{file_name}')

# COMMAND ----------

df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://raw@devdolphins.dfs.core.windows.net/ETE/checkpoints_{file_name}") \
    .option("path", f"abfss://raw@devdolphins.dfs.core.windows.net/ETE/{file_name}") \
    .trigger(once=True) \
    .start() 