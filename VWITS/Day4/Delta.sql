-- Databricks notebook source
Delta table.
1. SQL 
2. CTAS
3. Python
4. Dataframe

-- COMMAND ----------

create table vwits.emp(id int, name string)

-- COMMAND ----------

INSERT into vwits.emp values (1,'Naval')

-- COMMAND ----------

Internals of Delta 
parquet File (Data)
+
_delta_log (metadata)
(crc
json
)

-- COMMAND ----------

INSERT into vwits.emp values (2,'Ravi'),(3,'Akshay')

-- COMMAND ----------

INSERT into vwits.emp values (4,'Ashish');
INSERT into vwits.emp values (5,'Devesh');
INSERT into vwits.emp values (6,'Ritesh');

-- COMMAND ----------

describe detail vwits.emp

-- COMMAND ----------

select * from vwits.emp

-- COMMAND ----------

delete from vwits.emp where id =2 

-- COMMAND ----------

describe history vwits.emp

-- COMMAND ----------

select * from vwits.emp version as of 4

-- COMMAND ----------

select * from vwits.emp timestamp as of '2024-11-11T10:14:40.000+00:00'

-- COMMAND ----------

describe extended vwits.emp

-- COMMAND ----------


