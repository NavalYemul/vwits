-- Databricks notebook source
CREATE TABLE IF NOT EXISTS vwits.bronze.copy_into_table


-- COMMAND ----------

COPY INTO vwits.bronze.copy_into_table
FROM 'dbfs:/mnt/nyadls/raw/stream_in/'
FILEFORMAT = json
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

select * from vwits.bronze.copy_into_table
