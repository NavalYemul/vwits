# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://<container>@<storage_account>.blob.core.windows.net",
  mount_point = "/mnt/<storage_account>/<container>",
  extra_configs = {"fs.azure.account.key.<storage_account>.blob.core.windows.net":""})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@nyadls.blob.core.windows.net",
  mount_point = "/mnt/nyadls/raw",
  extra_configs = {"fs.azure.account.key.nyadls.blob.core.windows.net":"key"})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nyadls/raw/csv
