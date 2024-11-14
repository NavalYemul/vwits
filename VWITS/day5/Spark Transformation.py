# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

#df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

#df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DepTime", StringType(), True),
    StructField("CRSDepTime", IntegerType(), True),
    StructField("ArrTime", StringType(), True),
    StructField("CRSArrTime", IntegerType(), True),
    StructField("UniqueCarrier", StringType(), True),
    StructField("FlightNum", IntegerType(), True),
    StructField("TailNum", StringType(), True),
    StructField("ActualElapsedTime", StringType(), True),
    StructField("CRSElapsedTime", StringType(), True),
    StructField("AirTime", StringType(), True),
    StructField("ArrDelay", StringType(), True),
    StructField("DepDelay", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", StringType(), True),
    StructField("TaxiIn", StringType(), True),
    StructField("TaxiOut", StringType(), True),
    StructField("Cancelled", IntegerType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", IntegerType(), True),
    StructField("CarrierDelay", StringType(), True),
    StructField("WeatherDelay", StringType(), True),
    StructField("NASDelay", StringType(), True),
    StructField("SecurityDelay", StringType(), True),
    StructField("LateAircraftDelay", StringType(), True)
])

df = spark.read.csv("dbfs:/databricks-datasets/asa/airlines/", header=True, schema=schema)

# COMMAND ----------

#df.count()

# COMMAND ----------

df1=df.filter("Year=2007")

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

#df.select("Dest").distinct().display()

# COMMAND ----------

df.filter("Year=2007 and Dest='BGM'").display()

# COMMAND ----------

Transformations
1. Narrow 
 Filter
2. Shuffle (Wide)
  distinct, groupBy, join

# COMMAND ----------

df.groupBy("Year").count()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.sort(col("Year").desc())
