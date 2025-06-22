# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df = spark.read.format('parquet').load('abfss://raw@devdolphins.dfs.core.windows.net/ETE/products/')
# df = df.display()

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('abfss://processed@devdolphins.dfs.core.windows.net/ETE/products')

# COMMAND ----------

# df.createOrReplaceTempView('products')

# COMMAND ----------

# %sql
# CREATE OR REPLACE FUNCTION DATABRICKS_CATALOG.BRONZE.DISCOUNT_FUNC(p_price DOUBLE)
# RETURNS double 
# LANGUAGE SQL
# return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select product_id,price,DATABRICKS_CATALOG.BRONZE.DISCOUNT_FUNC(price) as discounted_price from products

# COMMAND ----------

# %sql
# create or replace function DATABRICKS_CATALOG.BRONZE.reverse_func(p_brand string)
# returns string
# language python
# as
# $$
#     return p_brand[::-1]
# $$

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select brand,DATABRICKS_CATALOG.BRONZE.reverse_func(brand) from products;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.silver_products  
# MAGIC using delta location 'abfss://processed@devdolphins.dfs.core.windows.net/ETE/products'