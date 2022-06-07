-- Databricks notebook source
-- MAGIC %md ###(A) Query using Spark Engine

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

-- MAGIC %md ###(B) Query using Photon Engine

-- COMMAND ----------

SELECT PickupLocationId
  , COUNT(*) AS TotalRides
  
FROM TaxisDB.YellowTaxis

WHERE TripYear = 2022
  AND TripMonth = 3
  AND TripDay = 15

GROUP BY PickupLocationId 

ORDER BY TotalRides DESC
