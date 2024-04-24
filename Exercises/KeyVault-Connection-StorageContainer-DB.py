# Databricks notebook source
# MAGIC %md
# MAGIC Bring data from storage container.

# COMMAND ----------

saskeyy = dbutils.secrets.get(scope='newsasKey', key='newsasKey')
storage_account_name = 'sq01blobtest'
source_container = "wasbs://testsource@sq01blobtest.core.windows.net/"
sink_container = "wasbs://testsink@sq01blobtest.core.windows.net/"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", saskeyy)
#spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", saskeyy)
#spark.conf.set(f"fs.azure.sas.testsource.{storage_account_name}.blob.core.windows.net", saskeyy)

# COMMAND ----------

spark.conf.set(f"fs.azure.sas.testsink.{storage_account_name}.blob.core.windows.net", saskeyy)

# COMMAND ----------

# MAGIC %fs ls wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7

# COMMAND ----------

# MAGIC %fs ls wasbs://testsink@sq01blobtest.blob.core.windows.net/

# COMMAND ----------

dbutils.fs.ls('wasbs://testsource@sq01blobtest.blob.core.windows.net')

# COMMAND ----------

dbutils.fs.ls('wasbs://testsink@sq01blobtest.blob.core.windows.net')

# COMMAND ----------

df = spark.read.json('wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7/flight-time.json')

# COMMAND ----------


