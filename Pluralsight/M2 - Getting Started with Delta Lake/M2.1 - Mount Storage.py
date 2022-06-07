# Databricks notebook source
# MAGIC %md ###Replace Values 
# MAGIC <br/>
# MAGIC 
# MAGIC 1. Replace Client Id, Client Secret, Tenant Id values in configs <br/>
# MAGIC 2. Replace container name and data lake name in mount function
# MAGIC 
# MAGIC <i>Note: Make sure to remove 3 stars before and after values

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "66a89324-d1ed-47f2-be8d-45fd89706268",
           "fs.azure.account.oauth2.client.secret": "Ysy8Q~OZVxeTNHDrnUhAnjA7H5l6zr~~s5M6Ja~G",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2f2e0da1-0d79-4d2b-b60c-9e541463f9f9/oauth2/token"}

# Mount the Data Lake Gen2 account
dbutils.fs.mount(
  source = "abfss://taxidata@pltaxidatalakeg2.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/datalake/Raw
