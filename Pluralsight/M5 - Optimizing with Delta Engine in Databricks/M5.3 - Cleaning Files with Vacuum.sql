-- Databricks notebook source
DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

VACUUM TaxisDB.YellowTaxis 

RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM TaxisDB.YellowTaxis 

RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

VACUUM TaxisDB.YellowTaxis 

RETAIN 0 HOURS

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

SELECT *
FROM TaxisDB.YellowTaxis   
VERSION AS OF 0
