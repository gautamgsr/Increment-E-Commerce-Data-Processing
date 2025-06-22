# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://raw@devdolphins.dfs.core.windows.net/ETE/customer/')

# COMMAND ----------

df = df.withColumn('domains',split(col('email'),'@')[1])
grouped_df = df.groupBy('domains').agg(count('customer_id').alias('total_customers')).orderBy(col('total_customers').desc())

# COMMAND ----------

# df_gmail = df.filter(col('domains') == 'gmail.com')
# df_gmail.display()

# df_yahoo = df.filter(col('domains') == 'yahoo.com')
# df_yahoo.display()

# df_hotmail = df.filter(col('domains') == 'hotmail.com')
# df_hotmail.display()

# COMMAND ----------

df = df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))
df = df.drop('first_name','last_name','_rescued_data')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('abfss://processed@devdolphins.dfs.core.windows.net/ETE/customer')


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.silver_customers
# MAGIC using delta location 'abfss://processed@devdolphins.dfs.core.windows.net/ETE/customer'