-- Databricks notebook source
-- MAGIC %md ###(A) Check if Delta Cache is enabled

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.io.cache.enabled")

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxis

WHERE TripYear = 2022
  AND TripMonth = 3
  AND TripDay = 15

GROUP BY PickupLocationId 

ORDER BY TotalRides DESC

-- COMMAND ----------

-- MAGIC %md ###(B) Enable Delta Cache

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxis

WHERE TripYear = 2022
  AND TripMonth = 3
  AND TripDay = 15

GROUP BY PickupLocationId 

ORDER BY TotalRides DESC

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxis

WHERE TripYear = 2022
  AND TripMonth = 3
  AND TripDay = 15

GROUP BY PickupLocationId 

ORDER BY TotalRides DESC

-- COMMAND ----------

-- MAGIC %md ###(C) Manually push data to Delta Cache

-- COMMAND ----------

CACHE    

SELECT *  
FROM TaxisDB.YellowTaxisParquet
WHERE TripDay = 1

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxisParquet

WHERE TripYear = 2022
  AND TripMonth = 3
  AND TripDay = 1

GROUP BY PickupLocationId 

ORDER BY TotalRides DESC
