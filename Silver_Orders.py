# Databricks notebook source
df = spark.read.format('parquet').load('abfss://raw@devdolphins.dfs.core.windows.net/ETE/orders/')

# COMMAND ----------

df = df.drop('_rescued_data')
df = df.withColumn('order_date',to_timestamp(col('order_date')))
df = df.withColumn('year',year(col('order_date')))
# df = df.withColumn('discount_price',)

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://processed@devdolphins.dfs.core.windows.net/ETE/orders')

# COMMAND ----------

# from pyspark.sql.functions import * 
# from pyspark.sql.window import Window

# COMMAND ----------

# class windows:
#     def dense_rank(self,df):
#         return df.withColumn('dense_rank',dense_rank().over(Window.partitionBy('year').orderBy(col('total_amount').desc())))
#     def rank(self,df):  
#         return df.withColumn('rank',rank().over(Window.partitionBy('year').orderBy(col('total_amount').desc())))
#     def row_number(self,df):
#         return df.withColumn('row_number',row_number().over(Window.partitionBy('year').orderBy(col('total_amount').desc())))




# COMMAND ----------

# obj = windows()

# COMMAND ----------

# ranked = obj.rank(df)
# ranked.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.silver_orders
# MAGIC using delta location 'abfss://processed@devdolphins.dfs.core.windows.net/ETE/orders'