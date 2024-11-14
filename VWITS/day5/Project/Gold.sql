-- Databricks notebook source
-- MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/VWITS/day5/Project/includes

-- COMMAND ----------

create or replace view gold.customer_total_amount as 
(select customer_id,customer_name,round(sum(discount_amount)) as total_discount, round(sum(total_amount)) as total_amount from vwits.silver.sales_customer group by all)

-- COMMAND ----------

create or replace view customer_count as 
(select customer_city, count(*) as customer_count from vwits.silver.sales_customer group by customer_city)
