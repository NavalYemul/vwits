-- Databricks notebook source
select * from datamaster.vwits.linkedin_job

-- COMMAND ----------

-- DBTITLE 1,Syntax
create function function_name(para datatype)
Returns datatype
return logic

-- COMMAND ----------

drop function job_description

-- COMMAND ----------

create or replace function job_description(type string)
returns string
return concat("The job is ",type )

-- COMMAND ----------

select *, job_description(job_type) as message from datamaster.vwits.linkedin_job

-- COMMAND ----------

 create function vwits.get_pay_description(country string)
 returns string
 return 
  case 
    when country="United States" THEN "High Pay"
    when country= "United Kingdom" THEN "GOOD pay"
    else 'Medium Pay'
  end;

-- COMMAND ----------

select *, vwits.get_pay_description(search_country) as message from datamaster.vwits.linkedin_job
