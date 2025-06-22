# Databricks notebook source
# MAGIC %md
# MAGIC # FACT ORDERS

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql('select * from databricks_catalog.silver.silver_orders')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact DataFrame**

# COMMAND ----------

df_dimcus = spark.sql('select dimCustomerKey,customer_id as dim_customer_id from databricks_catalog.gold.dimcustomers')


#below table is not existing as delta live tables are not working for me
df_dimpro = spark.sql('select product_id as dimProductKey,product_id as dim_product_id from databricks_catalog.gold.dimproducts')

df_dimcus.display()
# df_fact = 

# COMMAND ----------

df_fact = df.join(df_dimcus,df['customer_id'] == df_dimcus['dim_customer_id'],how='left').join(df_dimpro,df['product_id'] == df_dimpro['dim_product_id'],how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','dim_product_id','dim_customer_id')
df_fact_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on Fact table**

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("databricks_catalog.gold.FactOrders"):
  dlt_obj = DeltaTable.forName(spark, "databricks_catalog.gold.FactOrders")
  dlt_obj.alias('trg').merge(df_fact_new.alias('src'),'trg.order_id = src.order_id and trg.DimCustomerKey = src.DimCustomerKey and trg.DimProductKey = src.DimProductKey').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  DeltaTable.forTable(spark
else:
  df_fact_new.write.format("delta").option('path','abfss://published@devdolphins.dfs.core.windows.net/ETE/FactOrders').saveAsTable("databricks_catalog.gold.FactOrders")