-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC # Create schema for Yellow Taxis
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC   
-- MAGIC yellowTaxiSchema = (
-- MAGIC                       StructType()  
-- MAGIC                         .add("RideId", "integer")  
-- MAGIC                         .add("VendorId", "integer")
-- MAGIC                         .add("PickupTime", "timestamp")
-- MAGIC                         .add("DropTime", "timestamp")
-- MAGIC                         .add("PickupLocationId", "integer")
-- MAGIC                         .add("DropLocationId", "integer")
-- MAGIC                         .add("CabNumber", "string")
-- MAGIC                         .add("DriverLicenseNumber", "string")
-- MAGIC                         .add("PassengerCount", "integer")
-- MAGIC                         .add("TripDistance", "double")  
-- MAGIC                         .add("RatecodeId", "integer")
-- MAGIC                         .add("PaymentType", "integer")
-- MAGIC                         .add("TotalAmount", "double")  
-- MAGIC                         .add("FareAmount", "double")
-- MAGIC                         .add("Extra", "double")
-- MAGIC                         .add("MtaTax", "double")
-- MAGIC                         .add("TipAmount", "double")
-- MAGIC                         .add("TollsAmount", "double")                      
-- MAGIC                         .add("ImprovementSurcharge", "double")
-- MAGIC                    )

-- COMMAND ----------

-- MAGIC %md ###(A) UPDATE command

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.YellowTaxis

WHERE RideId = 9999997

-- COMMAND ----------

SELECT INPUT_FILE_NAME()
     
     , *
FROM TaxisDB.YellowTaxis

WHERE VendorId = 3
ORDER BY RideId

-- COMMAND ----------

UPDATE TaxisDB.YellowTaxis

SET PassengerCount = 2

WHERE RideId = 9999997

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.YellowTaxis

WHERE RideId = 9999997

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %md ###(B) DELETE command

-- COMMAND ----------

DELETE FROM TaxisDB.YellowTaxis

WHERE RideId = 9999999

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Extract changed records from Data Lake
-- MAGIC yellowTaxiChangesDF = (
-- MAGIC                            spark
-- MAGIC                              .read
-- MAGIC                              .option("header", "true")
-- MAGIC                              .schema(yellowTaxiSchema)
-- MAGIC                              .csv("/mnt/datalake/Raw/YellowTaxis_Additional/YellowTaxis_changes.csv")
-- MAGIC                        )
-- MAGIC 
-- MAGIC display(yellowTaxiChangesDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC yellowTaxiChangesDF.createOrReplaceTempView("YellowTaxiChanges")

-- COMMAND ----------

SELECT *
FROM YellowTaxiChanges

-- COMMAND ----------

MERGE INTO TaxisDB.YellowTaxis tgt

    USING YellowTaxiChanges    src

        ON    tgt.VendorId  =  src.VendorId
          AND tgt.RideId    =  src.RideId
  
-- Update row if join conditions match
WHEN MATCHED
      
      THEN  
          UPDATE SET    tgt.PaymentType = src.PaymentType                        -- Use 'UPDATE SET *' to update all columns

-- Insert row if row is not present in target table
WHEN NOT MATCHED 
      AND PickupTime >= '2022-03-01'

      THEN 
          INSERT (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, 
                  TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)
          
          VALUES (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, 
                  TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)

-- COMMAND ----------

SELECT * FROM TaxisDB.YellowTaxis
WHERE RideId > 9999995

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis
