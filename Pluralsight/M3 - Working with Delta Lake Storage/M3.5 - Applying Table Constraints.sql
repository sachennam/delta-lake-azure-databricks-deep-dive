-- Databricks notebook source
-- MAGIC %md ###(A) NOT NULL constraint
-- MAGIC 
-- MAGIC Define NOT NULL constraint on column to avoid insertion of NULL values

-- COMMAND ----------

ALTER TABLE TaxisDB.YellowTaxis

ALTER COLUMN RideId  SET NOT NULL

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------


INSERT INTO TaxisDB.YellowTaxis
(
    RideId, 
    
    VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, TripDistance, 
    RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge
)

VALUES 
(
    NULL, 

    3, '2021-12-01T00:00:00.000Z', '2021-12-01T00:15:34.000Z', 170, 140, 'TAC399', '5131685', 1, 2.9, 
    1, 1, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 0.3
)

-- COMMAND ----------

-- MAGIC %md ###(B) CHECK constraint
-- MAGIC 
-- MAGIC Check constraint helps to enforce certain conditions on the table

-- COMMAND ----------


ALTER TABLE TaxisDB.YellowTaxis

    ADD CONSTRAINT PassengerCheck CHECK (PassengerCount <= 5)

-- COMMAND ----------

ALTER TABLE TaxisDB.YellowTaxis

    ADD CONSTRAINT PassengerCheck CHECK (PassengerCount <= 9) 
    -- Multiple conditions: (PassengerCount <= 9 OR PassengerCount IS NULL)

-- COMMAND ----------

INSERT INTO TaxisDB.YellowTaxis
(
    RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber,
    
    PassengerCount,
    
    TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge
)

VALUES 
(
    10000001, 3, '2021-12-01T00:00:00.000Z', '2021-12-01T00:15:34.000Z', 170, 140, 'TAC399', '5131685', 
    
    null,
    
    2.9, 1, 1, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 0.3
)

-- COMMAND ----------

-- MAGIC %md ###(C) Drop constraint

-- COMMAND ----------

ALTER TABLE TaxisDB.YellowTaxis

    DROP CONSTRAINT PassengerCheck

-- COMMAND ----------


DESCRIBE HISTORY TaxisDB.YellowTaxis
