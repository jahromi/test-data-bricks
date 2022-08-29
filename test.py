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
# MAGIC SELECT * FROM default.people10m TIMESTAMP AS OF current_timestamp()
