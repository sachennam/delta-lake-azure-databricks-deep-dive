-- Databricks notebook source
-- MAGIC %md ###(A) Update a record

-- COMMAND ----------


SELECT RideId, PassengerCount
FROM TaxisDB.YellowTaxis
WHERE RideId = 10000000



-- COMMAND ----------

-- COMMAND ----------

UPDATE TaxisDB.YellowTaxis
SET PassengerCount = 1
WHERE RideId = 10000000;

-- COMMAND ----------

SELECT RideId, PassengerCount
FROM TaxisDB.YellowTaxis
WHERE RideId = 10000000

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %md ###(B) Access using Version Number

-- COMMAND ----------

SELECT RideId, PassengerCount

FROM TaxisDB.YellowTaxis        

VERSION AS OF 0

WHERE RideId = 10000000

-- COMMAND ----------

SELECT RideId, PassengerCount

FROM TaxisDB.YellowTaxis        

VERSION AS OF 7

WHERE RideId = 10000000

-- COMMAND ----------

-- MAGIC %md ###(C) Access using Timestamp

-- COMMAND ----------


SELECT RideId, PassengerCount

FROM TaxisDB.YellowTaxis        TIMESTAMP AS OF '2022-06-04T18:07:32.000+0000'

WHERE RideId = 10000000

-- COMMAND ----------

-- MAGIC %md ###(D) Restore Table to older version

-- COMMAND ----------

RESTORE TABLE TaxisDB.YellowTaxis    TO VERSION AS OF 10

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

SELECT RideId, PassengerCount
FROM TaxisDB.YellowTaxis
WHERE RideId = 10000000
