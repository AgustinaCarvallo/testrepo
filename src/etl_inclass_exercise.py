# Databricks notebook source
# MAGIC %md #### Workshop for ETL

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date,avg
from pyspark.sql.types import IntegerType


# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform Data

# COMMAND ----------

df_driver = df_driver.withColumn("age", datediff(current_date(), df_driver.dob)/365)

# COMMAND ----------

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select('nationality', 'age', 'forename', 'surname', 'url', 'driverId').join(df_laptimes, on = ['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate by Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupBy('nationality', 'age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Storing Data in S3

# COMMAND ----------

df_lap_drivers.write.csv('s3://ac5356-gr5069/processed/in_class_workshop_2024/laptimes_by_drivers.csv')

# COMMAND ----------


