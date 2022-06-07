-- Databricks notebook source
-- MAGIC %md ###(A) Create Live Bronze Table

-- COMMAND ----------

CREATE LIVE TABLE TaxiZones_BronzeLive

LOCATION "/mnt/datalake/Output/TaxiZones_BronzeLive.delta"

COMMENT "Live Bronze table for Taxi Zones"

AS

SELECT *
FROM parquet.`/mnt/datalake/Raw/TaxiZones.parquet`

-- COMMAND ----------

-- MAGIC %md ###(B) Create Live Bronze View

-- COMMAND ----------

CREATE LIVE VIEW TaxiZones_SilverLive
(
    CONSTRAINT Valid_LocationId    EXPECT (LocationId IS NOT NULL AND LocationId > 0) ON VIOLATION DROP ROW
)

COMMENT "Live Bronze view for Taxi Zones"

AS

SELECT *
FROM live.TaxiZones_BronzeLive

