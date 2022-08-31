# Databricks notebook source
# Define the input and output formats and paths and the table name.
read_format = 'delta'
write_format = 'delta'
load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
save_path = 's3://data-brick-test-inovation-week/delta'
table_name = 'default.people10m'

# Load the data from its source.
people = spark \
  .read \
  .format(read_format) \
  .load(load_path)

# Write the data to its target.
people.write \
  .format(write_format) \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

spark.sql("drop table " + table_name)

# COMMAND ----------

people = spark.read.format('delta').load('s3://data-brick-test-inovation-week/delta')
people.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.people10m VALUES
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25', '567-89-0123', 89900)

# COMMAND ----------

df1 = spark.read.format('delta').option('timestampAsOf', '2022-08-29').load('s3://data-brick-test-inovation-week/delta')

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe default.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from default.people10m 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.people10m TIMESTAMP AS OF '2023-08-29 23:20:21'

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
SparkSession\
.builder\
.master("local")\
.appName("spark session")\
.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")\
.getOrCreate()

# COMMAND ----------

checkpoint_path = 's3://data-brick-test-inovation-week/auto-load-target/_checkpoints'
write_path = 's3://data-brick-test-inovation-week/auto-load-target'
upload_path = 's3://data-brick-test-inovation-week/auto-load-source'

# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .schema('city string, year int, population long') \
  .load(upload_path)

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that
# have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check,
# write the newly-uploaded files' data to the write_path location.
df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

spark.sql("CREATE TABLE default.city_population USING DELTA LOCATION 's3://data-brick-test-inovation-week/auto-load-target'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.city_population version as of 5

# COMMAND ----------

import boto3
s3 = boto3.client('s3')
response = s3.list_buckets()
print(response)

# COMMAND ----------

import dlt

@dlt.table
def filtered_data():
  return dlt.read("taxi_raw").where(...)

