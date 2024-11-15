# Databricks notebook source
data=([(1,'Sachin'),(2,'Sehwag'),(3,'Virat'),(4,'Rahul')])
schema="id int,name string"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.write.saveAsTable("players")

# COMMAND ----------

data1=([(5,'Bummrah','Blower'),(6,'Dhoni','Batsman')])
schema1="id int,name string,dept string"
df1=spark.createDataFrame(data1,schema1)

# COMMAND ----------

df1.write.saveAsTable("players")

# COMMAND ----------

df1.write.mode("append").saveAsTable("players")

# COMMAND ----------

df1.write.mode("append").option("mergeSchema", "true").saveAsTable("players")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO players values(7,'Sky','Batsman')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from players

# COMMAND ----------

data2=([(1,'Sachin','Batsmen'),(6,'Dhoni','Wicketkeeping'),(8,'Shubnam','Batsmen')])
schema2="id int,name string,dept string"
df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df2.createOrReplaceTempView("player_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO players t
# MAGIC USING player_temp s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC   t.name = s.name,
# MAGIC   t.dept=s.dept
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     id,
# MAGIC     name,
# MAGIC     dept
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     id,
# MAGIC     name,
# MAGIC     dept
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from players

# COMMAND ----------

# Assuming we have a target table 'player' with the same schema as 'player_temp'
# and we want to implement SCD Type 1 to update the target table with the latest data from 'player_temp'

# Load the target table
target_df = spark.table("player")

# Merge the data from player_temp into the target table
merge_query = """
MERGE INTO player AS target
USING player_temp AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET target.name = source.name, target.dept = source.dept
WHEN NOT MATCHED THEN
  INSERT (id, name, dept) VALUES (source.id, source.name, source.dept)
"""

# Execute the merge query
spark.sql(merge_query)
