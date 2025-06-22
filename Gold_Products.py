# Databricks notebook source
# MAGIC %md
# MAGIC # **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC streaming **table**

# COMMAND ----------

#Expectations
my_rules = {
    'rule1' : 'product_id IS NOT NULL',
    'rule2' : 'product_name IS NOT NULL'
}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(name='dimProducts_stage')
@dlt.expect_all_or_drop(my_rules)
def dimProducts_stage():
    df = spark.readStream.table('databricks_catalog.sliver.silver_products')
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming View**

# COMMAND ----------

@dlt.view(name='dimProducts_view')
def dimProducts_view():

    df = spark.readStream.table('Live.dimProducts_stage')

# COMMAND ----------

# MAGIC %md
# MAGIC **Dim Products**

# COMMAND ----------

dlt.create_streaming_table('DimProducts')

# COMMAND ----------

dlt.apply_changes(
    target='DimProducts',
    source='dimProducts_view',
    keys=['product_id'],
    sequence_by=col('product_id'),
    stored_as_scd_type= 2
)