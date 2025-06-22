# Databricks notebook source
df = spark.read.table('databricks_catalog.bronze.region')

# COMMAND ----------

df.drop('_rescued_data')

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('abfss://processed@devdolphins.dfs.core.windows.net/ETE/region')
# df.write.format('delta').mode('append').save('abfss://processed@devdolphins.dfs.core.windows.net/ETE/orders')

# COMMAND ----------

# df_cust = spark.read.format('delta').load('abfss://processed@devdolphins.dfs.core.windows.net/ETE/customer')
# df_region = spark.read.format('delta').load('abfss://processed@devdolphins.dfs.core.windows.net/ETE/orders')
# df_product = spark.read.format('delta').load('abfss://processed@devdolphins.dfs.core.windows.net/ETE/products')
# df_orders = spark.read.format('delta').load('abfss://processed@devdolphins.dfs.core.windows.net/ETE/orders')
# df_cust.display()
# df_orders.display()
# df_region.display()
# df_product.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.silver_regions
# MAGIC using delta location 'abfss://processed@devdolphins.dfs.core.windows.net/ETE/regions'