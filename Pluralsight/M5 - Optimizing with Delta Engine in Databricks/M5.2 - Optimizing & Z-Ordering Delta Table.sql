-- Databricks notebook source
-- MAGIC %md ###(A) Check performance before optimization

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxisParquet

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

-- MAGIC %md ###(B) Optimize: Bin-packing

-- COMMAND ----------

OPTIMIZE TaxisDB.YellowTaxis

WHERE PickupLocationId = 100              -- optional

-- COMMAND ----------

-- MAGIC %md ###(C) Optimize: Z-Ordering

-- COMMAND ----------

OPTIMIZE TaxisDB.YellowTaxis

ZORDER BY TripYear, TripMonth, TripDay    -- optional


-- COMMAND ----------

-- MAGIC %md ###(D) Check performance after optimization

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


