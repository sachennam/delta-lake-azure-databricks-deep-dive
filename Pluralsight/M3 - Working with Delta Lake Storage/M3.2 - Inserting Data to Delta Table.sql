-- Databricks notebook source
-- MAGIC %md ####Option 1: Insert command

-- COMMAND ----------

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

INSERT INTO TaxisDB.YellowTaxis

(RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)

VALUES (9999996, 3, '2021-12-01T00:00:00.000Z', '2021-12-01T00:15:34.000Z', 170, 140, 'TAC399', '5131685', 1, 2.9, 1, 1, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 0.3)


-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md ####Option 2: Append a DataFrame
-- MAGIC 
-- MAGIC Read data as a DataFrame and append to Delta Table using PySpark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC yellowTaxisAppendDF = (
-- MAGIC                           spark
-- MAGIC                             .read
-- MAGIC                             .option("header", "true")
-- MAGIC                             .schema(yellowTaxiSchema)
-- MAGIC                             .csv("/mnt/datalake/Raw/YellowTaxis_Additional/YellowTaxis_append.csv")
-- MAGIC                       )
-- MAGIC 
-- MAGIC display(yellowTaxisAppendDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Append to data lake in delta format
-- MAGIC (
-- MAGIC     yellowTaxisAppendDF
-- MAGIC         .write
-- MAGIC   
-- MAGIC         .mode("append")
-- MAGIC   
-- MAGIC         .partitionBy("VendorId")  
-- MAGIC         .format("delta")           
-- MAGIC         .save("/mnt/datalake/Output/YellowTaxis.delta")
-- MAGIC )

-- COMMAND ----------


SELECT * 
FROM TaxisDB.YellowTaxis
ORDER BY RideId

-- COMMAND ----------

-- MAGIC %md ####Option 3: Copy Command
-- MAGIC 
-- MAGIC Use COPY command to copy from Data Lake to Delta Table

-- COMMAND ----------

COPY INTO TaxisDB.YellowTaxis

    FROM '/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv'             -- If folder is provided in FROM clause, files can be specified
                                                                      -- Example: FILES = ('f1.csv', 'f2.csv', 'f3.csv')
    
    FILEFORMAT = CSV                                                  -- Other options: JSON, PARQUET, AVRO, ORC, TEXT, BINARYFILE
    
    VALIDATE ALL                                                      -- Other option: VALIDATE 10 ROWS
    
    FORMAT_OPTIONS ('header' = 'true')                                -- Several format options are available
                                                                      -- Generic and specific to file format


-- COMMAND ----------


COPY INTO TaxisDB.YellowTaxis
FROM (
        SELECT RideId::Int
        
             , VendorId::Int
             , PickupTime::Timestamp
             , DropTime::Timestamp
             , PickupLocationId::Int
             , DropLocationId::Int
             , CabNumber::String
             , DriverLicenseNumber::String
             , PassengerCount::Int
             , TripDistance::Double
             , RateCodeId::Int
             , PaymentType::Int
             , TotalAmount::Double
             , FareAmount::Double
             , Extra::Double
             , MtaTax::Double
             , TipAmount::Double
             , TollsAmount::Double
             , ImprovementSurcharge::Double
             
        FROM '/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv' 
    )    
    
    FILEFORMAT = CSV    
    FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

COPY INTO TaxisDB.YellowTaxis
FROM (
        SELECT RideId::Int
        
             , VendorId::Int
             , PickupTime::Timestamp
             , DropTime::Timestamp
             , PickupLocationId::Int
             , DropLocationId::Int
             , CabNumber::String
             , DriverLicenseNumber::String
             , PassengerCount::Int
             , TripDistance::Double
             , RateCodeId::Int
             , PaymentType::Int
             , TotalAmount::Double
             , FareAmount::Double
             , Extra::Double
             , MtaTax::Double
             , TipAmount::Double
             , TollsAmount::Double
             , ImprovementSurcharge::Double
             
        FROM '/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv' 
    )
    
    FILEFORMAT = CSV    
    FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

-- DO NOT RUN. THIS WILL DUPLICATE THE DATA
COPY INTO TaxisDB.YellowTaxis
FROM (
        SELECT RideId::Int        
             , VendorId::Int
             , PickupTime::Timestamp
             , DropTime::Timestamp
             , PickupLocationId::Int
             , DropLocationId::Int
             , CabNumber::String
             , DriverLicenseNumber::String
             , PassengerCount::Int
             , TripDistance::Double
             , RateCodeId::Int
             , PaymentType::Int
             , TotalAmount::Double
             , FareAmount::Double
             , Extra::Double
             , MtaTax::Double
             , TipAmount::Double
             , TollsAmount::Double
             , ImprovementSurcharge::Double
             
        FROM '/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv' 
    )    
    FILEFORMAT = CSV    
    FORMAT_OPTIONS ('header' = 'true')
    
    COPY_OPTIONS ('force' = 'true')
