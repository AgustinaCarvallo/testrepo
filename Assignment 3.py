# Databricks notebook source
# MAGIC %md
# MAGIC ##Assignment #3

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing functions and dataframes to answer the questions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, current_date, avg, sum, when, col, rank, desc, max, min, round
from pyspark.sql.window import Window

# COMMAND ----------

#Importing the data frames
df_pitstops = spark.read.csv('s3://columbia-gr5069-main/raw/pit_stops.csv', header=True)
df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header=True)
df_results = spark.read.csv('s3://columbia-gr5069-main/raw/results.csv', header=True)
df_status = spark.read.csv('s3://columbia-gr5069-main/raw/status.csv', header=True)
df_circuits = spark.read.csv('s3://columbia-gr5069-main/raw/circuits.csv', header=True)

# COMMAND ----------

#I used this line to visualize the different data frames imported before
display(df_pitstops)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 1

# COMMAND ----------

# MAGIC %md
# MAGIC What was the average time each driver spent at the pit stop for each race?

# COMMAND ----------

#First I grouped df_pitstops by columns "raceid" and "driverid". Then I used the avg function to calculate the average of  milliseconds spent at the pit stop for each group. Also, for convenience, I transformed them to seconds. The new column avg_pitstop displayed below shows how many seconds each driver spent on average at the pit stop for each race. 

avg_pitstop = df_pitstops.groupBy("raceid", "driverid").agg((avg("milliseconds") / 1000).alias("avg_pitstop"))
display(avg_pitstop)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2

# COMMAND ----------

# MAGIC %md
# MAGIC Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

#Here I joined avg_pitstop and df_results, and selected my columns of interest in order to answer the question. Then I sorted the new df in ascending order, based on raceId and positionOrder. As a result, I have a new data frame (ranked_avg) where the average time spent at the pit stop is ranked in order of the driver that won each race.

ranked_avg = avg_pitstop.join(df_results.select('positionOrder', 'driverId', 'raceId'), on=['driverId', 'raceId'])
ranked_avg = ranked_avg.withColumn("positionOrder", ranked_avg["positionOrder"].cast("int"))
ranked = ranked_avg.orderBy(col('raceId'), col('positionOrder'))

display(ranked_avg)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 3

# COMMAND ----------

# MAGIC %md
# MAGIC Insert the missing code (e.g: ALO for Alonso) for drivers based on the 'drivers' dataset

# COMMAND ----------

#First, I filtered for missing codes, removed whitespaces in surnames and accents over letters.
df_missing_code = df_drivers.where(col('code').isNull() | (col('code') == "\\N"))

df_missing_code = df_missing_code.withColumn('surname', regexp_replace(df_missing_code['surname'], " ",""))

@F.pandas_udf('string')
def strip_accents(s: pd.Series) -> pd.Series:
    return s.str.normalize('NFKD').str.encode('ascii','ignore').str.decode('utf-8')
df_missing_code = df_missing_code.withColumn('surname', strip_accents('surname'))

#Then I determined the codes based on the pattern, and joined them to the df_drivers dataframe.
df_missing_code = df_missing_code.withColumn('code',upper(substring(df_missing_code['surname'],1,3)))

df_missing_code_select = df_missing_code.select('driverId','code')

df_drivers_complete = df_drivers.join(df_missing_code_select.withColumnRenamed('code','missing_code'), 'driverId', 'left') \
    .withColumn('code', coalesce(col('missing_code'),col('code'))) \
        .drop('missing_code')

# COMMAND ----------

#Now that I have filled in the missing codes, I need to deal with duplicates. I will add a number to ensure that each driver has a unique code. For example, alboreto gets ALB, and albers gets ALB-2, etc. 

#For this, I first find the duplicates
window_spec = Window.partitionBy('code')
df_code_counts = df_drivers_complete.withColumn('count', count('*').over(window_spec))

#Then, I add a column which counts the number of rows with that code and specifies True or False based on whether it is a duplicate or not 
df_conditioned = df_code_counts.withColumn('is_duplicate', when(col('count') > 1, True).otherwise (False))

#Here I add a row number to each row within each group of duplicates
#I partition by column code and order by driverId
window_spec = Window.partitionBy('code').orderBy('driverId')
df_with_row_number = df_conditioned.withColumn('row_number', row_number().over(window_spec))

df_updated_codes = df_with_row_number.withColumn('code',
    when(col('is_duplicate') & (col('row_number') ==1), df_with_row_number['code'])
     .when(col('is_duplicate'), concat(df_with_row_number['code'], lit('-'), col('row_number')))
     .otherwise(df_with_row_number['code'])
    ).drop('row_number', 'is_duplicate','count')

display(df_updated_codes)

#Finally, in the df_updated_codes dataframe displayed here, I have solved the issue of missing codes by filling them in and assigning a number to those that were duplicated so that each driver has its own code. 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 4

# COMMAND ----------

# MAGIC %md
# MAGIC Who is the youngest and oldest driver for each race? Create a new column called “Age”

# COMMAND ----------

#First I will join df_results with df_drivers and df_race_ages with df_races
df_results_drivers= df_results.join(df_drivers.select('forename', 'surname', 'driverID', 'dob'), on=['driverId'])
df_race_ages = df_results_drivers.join(df_races.select('date', 'raceID'), on=['raceId'])

#Then I calculate age
df_race_ages = df_race_ages.withColumn('age', round(datediff(df_race_ages.date, df_race_ages.dob) / 365).cast("int"))
display(df_race_ages)


# COMMAND ----------

#Now I calculate the max and min ages per race
df_max = df_race_ages.groupBy('raceId').agg(max('age').alias('age'))
df_min = df_race_ages.groupBy('raceId').agg(min('age').alias('age'))

df_max_min = df_max.union(df_min)

#And I join it with df_race_ages to get the other columns
df_merged = df_max_min.join(df_race_ages, ['raceId', 'age'], 'inner')
df_merged = df_merged.select('raceId', 'age', 'forename', 'surname', 'dob', 'date', 'driverId')
display(df_merged)

#As a result, in the df displayed below, for each race i have the max and min ages and the names of the drivers. For example for race 1, Ribens Barrichello was the oldest driver with 37 years and Sébastien Buemi was the youngest, with 20.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 5

# COMMAND ----------

# MAGIC %md
# MAGIC For a given race (circuit), which driver has the most wins and losses?

# COMMAND ----------

#Here I am analyzing for circuit 1. For this given race, Michael Schumacher had the most wins (4) and Fernando Alonso had the most losses (17). To find this, I filtered the results for circuit 1, then grouped the results by driver to count wins and losses. Then, I joined with the driver information and ordered by wins and losses to find the top drivers for these categories. 

results_for_circuit = results_with_circuit.where(col("circuitId") == 1)

wins_losses_per_driver = results_for_circuit.groupBy("driverId", "circuitId").agg(
    count(when(results_for_circuit["positionOrder"] == 1, True)).alias("wins"),
    count(when(results_for_circuit["positionOrder"] != 1, True)).alias("losses")
)

wins_losses_per_driver = wins_losses_per_driver.join(df_drivers, "driverId", "left")

most_wins_for_circuit = wins_losses_per_driver.orderBy(col("wins").desc()).limit(1)
most_losses_for_circuit = wins_losses_per_driver.orderBy(col("losses").desc()).limit(1)

most_wins_for_circuit_selected = most_wins_for_circuit.select("circuitId","driverId", "wins", "forename", "surname")
display(most_wins_for_circuit_selected)
most_losses_for_circuit_selected = most_losses_for_circuit.select("circuitId","driverId", "losses", "forename", "surname")
display(most_losses_for_circuit_selected)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 6

# COMMAND ----------

# MAGIC %md
# MAGIC Continue exploring the data by answering your own question:
# MAGIC "What is the distribution of drivers' nationalities across circuits?"

# COMMAND ----------

#For this part, I decided to analyze the percentage of drivers from each nationality for each circuit. I want to calculate the distribution of drivers with different nationalities across circuits to understand the level of diversity in these races. To do so, I first grouped the data by circuit and nationality to count the number of drivers from each nationality  in each circuit, then I calculated the total number of drivers per circuit and joined the dataframes. Then I calculated the percentage of drivers from each nationality per circuit. Finally, in the percentage_df dataframe, we can observe, for each circuit, the percentage of drivers from each of the nationalities in the data. For example, in circuit 1, 16% of the drivers were German and only 0.1% were Indonesian. There were drivers from 33 different nationalities participating in that circuit!

nationality_count_df = joined_df.groupBy("rc.circuitId", "nationality").agg(count("d.driverId").alias("driver_count"))

total_drivers_df = joined_df.groupBy("rc.circuitId").agg(count("d.driverId").alias("total_drivers"))

percentage_df = nationality_count_df.join(
    total_drivers_df,
    nationality_count_df["circuitId"] == total_drivers_df["circuitId"],
    "inner"
).select(
    nationality_count_df["circuitId"],
    nationality_count_df["nationality"],
    (col("driver_count") / col("total_drivers") * 100).alias("percentage")
)

display(percentage_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Storing in S3

# COMMAND ----------

percentage_df.write.csv('s3://ac5356-gr5069/processed/assignment3/percentage_df.csv')
df_updated_codes.write.csv('s3://ac5356-gr5069/processed/assignment3/df_updated_codes.csv')
df_merged.write.csv('s3://ac5356-gr5069/processed/assignment3/df_merged.csv')

