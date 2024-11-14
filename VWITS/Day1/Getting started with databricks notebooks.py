# Databricks notebook source
# DBTITLE 1,Python
print("run python")

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Run Scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test.emp(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test.emp values(1,'naval'),(2,'Ritesh'),(3,'Kiran')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.emp
