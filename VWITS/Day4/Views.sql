-- Databricks notebook source
Views : A virtual tables
1.standard view (SQL)
2.temp view (SQl, pyspark)
3.Global temp view (SQL, pyspark)

-- COMMAND ----------

create view vwits.linkedin_job as 
(select job_title, company,job_location,search_country,job_type from datamaster.vwits.linkedin_job_posting)

-- COMMAND ----------

create temp view linkedin_job_us as 
select * from vwits.linkedin_job where search_country ='United States'

-- COMMAND ----------

select * from linkedin_job_us

-- COMMAND ----------

show views

-- COMMAND ----------

create global temp view linked_job_country_wise as 
(select search_country, count(*) as country_count from datamaster.vwits.linkedin_job group by search_country order by country_count desc)

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.linked_job_country_wise
