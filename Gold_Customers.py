# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading from Source**

# COMMAND ----------

df = spark.sql('select * from databricks_catalog.silver.silver_customers')


# COMMAND ----------

# MAGIC %md
# MAGIC **Removing duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC # **Dividing new vs old records**

# COMMAND ----------

init_load_flag = dbutils.widgets.get('init_load_flag')
init_load_flag = int(init_load_flag)

# COMMAND ----------


# if init_load_flag==0:
#   df_old = spark.sql('select dimCustomerKey,customer_id,create_date,update_date from databricks_catalog.gold.DimCustomers')
# else:
#   df_old = spark.sql('select 0 dimCustomerKey,0 customer_id,CURRENT_TIMESTAMP create_date,CURRENT_TIMESTAMP update_date from databricks_catalog.silver.silver_customers where 1 = 0')


# COMMAND ----------

import time
if not spark.catalog.tableExists('databricks_catalog.gold.DimCustomers'):
  df_old = spark.sql('select dimCustomerKey,customer_id,create_date,update_date from databricks_catalog.gold.DimCustomers')
else:
  df_old = spark.sql('select 0 dimCustomerKey,0 customer_id,CURRENT_TIMESTAMP create_date,CURRENT_TIMESTAMP update_date from databricks_catalog.silver.silver_customers where 1 = 0')


# COMMAND ----------

df_old = df_old.withColumnRenamed('customer_id','customer_id_old').withColumnRenamed('dimCustomerKey','dimCustomerKey_old').withColumnRenamed('create_date','create_date_old').withColumnRenamed('update_date','update_date_old')

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Joins with old_df**

# COMMAND ----------

df_join = df.join(df_old,df.customer_id==df_old.customer_id_old,'left')
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating new vs old records**

# COMMAND ----------

df_new = df_join.filter(df_join.dimCustomerKey_old.isNull())
df_old = df_join.filter(df_join.dimCustomerKey_old.isNotNull())

# COMMAND ----------

# df_new.limit(5).display()
# df_old.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# dropping all the columns which are not required 
df_old = df_old.drop('customer_id_old','update_date_old')



#renaming old_create column to create_date 
df_old = df_old.withColumnRenamed('create_date_old','create_date')
df_old = df_old.withColumnRenamed('dimCustomerKey_old','dimCustomerKey')


#Recreating "update_date" column with current timestamp
df_old = df_old.withColumn('update_date',current_timestamp())

# COMMAND ----------

# df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

# dropping all the columns which are not required 
df_new = df_new.drop('DimCustomerKey_old','customer_id_old','update_date_old','create_date_old')

#Recreating "update_date" column with current timestamp
df_new = df_new.withColumn('update_date',current_timestamp())
df_new = df_new.withColumn('create_date',current_timestamp())

# COMMAND ----------

# df_new.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key from 1 for new records as they are new **

# COMMAND ----------

df_new = df_new.withColumn('dimCustomerKey',monotonically_increasing_id()+lit(1))


# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key by adding last max value**

# COMMAND ----------

if init_load_flag==1:
  max_surrogate_key = 0
else:
  df_max_surrogate_key = spark.sql('select max(dimCustomerKey) from databricks_catalog.gold.DimCustomers')
  max_surrogate_key = df_max_surrogate_key.collect()[0][0]

df_new = df_new.withColumn('dimCustomerKey',col('dimCustomerKey')+lit(max_surrogate_key))

# COMMAND ----------

# MAGIC %md
# MAGIC **Union of df new and df old**

# COMMAND ----------

# df_old.limit(5).display()
# df_new.limit(5).display()

df_final = df_old.unionByName(df_new)
# df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Dimension Surrogate key to optimize joins**

# COMMAND ----------

# from pyspark.sql.functions import *

# df = df.withColumn('dimCustomerKey',monotonically_increasing_id()+lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD Type 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if not spark.catalog.tableExists('databricks_catalog.gold.DimCustomers'):
  df_final.write.format('delta').mode('overwrite').option('path','abfss://published@devdolphins.dfs.core.windows.net/ETE/DimCustomers').saveAsTable('databricks_catalog.gold.DimCustomers')
else:
  dlt_obj = DeltaTable.forPath(spark,'abfss://published@devdolphins.dfs.core.windows.net/ETE/DimCustomers')
  dlt_obj.alias('trg').merge(df_final.alias('src'),'trg.dimCustomerKey = src.dimCustomerKey')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.DimCustomers;