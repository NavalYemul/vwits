-- Databricks notebook source
-- MAGIC %md
-- MAGIC Delta lake
-- MAGIC
-- MAGIC optimize 
-- MAGIC z order
-- MAGIC
-- MAGIC liquid clusting
-- MAGIC

-- COMMAND ----------

--make changes and read bronze table
select * from sales


select * from bronze.sales


df=spark.read.csv("path")
