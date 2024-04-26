# Databricks notebook source
saskeyy = dbutils.secrets.get(scope='sasKeyy', key='sasKeyy')
storage_account_name = 'sq01blobtest'
source_container = "testsource"
sink_container = "testsink"

# COMMAND ----------

spark.conf.set(f"fs.azure.sas.testsource.{storage_account_name}.blob.core.windows.net", saskeyy)
spark.conf.set(f"fs.azure.sas.testsink.{storage_account_name}.blob.core.windows.net", saskeyy)

# COMMAND ----------

# MAGIC %fs ls wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.json('wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7/people.json')
#df.printSchema()
#dftwo.dtypes
df.show()

# COMMAND ----------

dftwo = spark.read.json('wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7/people_2.json')
#dftwo.printSchema()
#dftwo.dtypes
dftwo.show()


# COMMAND ----------

df = df.withColumn('phone', lit(None).cast('string'))
dfunion = df.union(dftwo)
dfunion.show()

# COMMAND ----------

df3 = spark.read.json('wasbs://testsource@sq01blobtest.blob.core.windows.net/01-Data/dataset_ch7/people_3.json')
df3.show()

# COMMAND ----------

dfunion = dfunion.union(df3)
dfunion.show()

# COMMAND ----------

def create_mail(fname, lname, id):
    fixname = ''
    fixlname = ''
    if fname:
        fixname = fname.strip().lower().replace(' ', '_')
    elif lname:
        fixlname = fname.strip().lower().replace(' ', '_')
    
    return f'{fixname}.{fixlname}.{id}@mail.com'

udf_create_mail = udf(create_mail, StringType())



# COMMAND ----------

df_email = dfunion.withColumn('email', udf_create_mail(dfunion['fname'], dfunion['lname'], dfunion['id']))
df_email.select(col('email')).show()

# COMMAND ----------

df_email.show()

# COMMAND ----------

df_fixname = df_email.withColumn('fname', lower(regexp_replace(df_email['fname'], ' ', '_')))
df_fixname.select(col('fname')).show()

# COMMAND ----------

df_fixlname = df_fixname.withColumn('lname', lower(df_fixname['lname']))
df_fixlname.show()

# COMMAND ----------

from datetime import datetime

print(dir(datetime))
#print(help(datetime.strftime))

# COMMAND ----------

def fix_date(date):
    try:
        if date:
            datefix = datetime.strftime(datetime.strptime(date, "%Y-%m-%d"), "%Y-%m-%d")
            return datefix
    except:
        return None

udf_fix_date = udf(fix_date, StringType())

# COMMAND ----------

df_fix_date1 = df_fixlname.withColumn('date', udf_fix_date(df_fixlname['dob']))
df_fix_date1 = df_fix_date1.drop(col('dob'))
df_fix_date1.show()

# COMMAND ----------


