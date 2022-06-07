-- Databricks notebook source
-- MAGIC %md ###(A) Create Tables

-- COMMAND ----------

-- MAGIC %md ####(A.1) Create Bronze table

-- COMMAND ----------

CREATE TABLE TaxisDB.YellowTaxis_Bronze
(
    RideId                  INT,
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,
    CabNumber               STRING,
    DriverLicenseNumber     STRING,
    PassengerCount          INT,
    TripDistance            DOUBLE,
    RatecodeId              INT,
    PaymentType             INT,
    TotalAmount             DOUBLE,
    FareAmount              DOUBLE,
    Extra                   DOUBLE,
    MtaTax                  DOUBLE,
    TipAmount               DOUBLE,
    TollsAmount             DOUBLE,         
    ImprovementSurcharge    DOUBLE, 
    
    FileName                STRING,
    CreatedOn               TIMESTAMP
)
USING DELTA
LOCATION "/mnt/datalake/Output/YellowTaxis_Bronze.delta"

PARTITIONED BY (VendorId)
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md ####(A.2) Create Silver table

-- COMMAND ----------

CREATE TABLE TaxisDB.YellowTaxis_Silver
(
    RideId                  INT,
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,
    CabNumber               STRING,
    DriverLicenseNumber     STRING,
    PassengerCount          INT,
    TripDistance            DOUBLE,    
    TotalAmount             DOUBLE,
    
    PickupYear              INT              GENERATED ALWAYS AS (YEAR  (PickupTime)),
    PickupMonth             INT              GENERATED ALWAYS AS (MONTH (PickupTime)),
    PickupDay               INT              GENERATED ALWAYS AS (DAY   (PickupTime)),
    
    CreatedOn               TIMESTAMP,
    ModifiedOn              TIMESTAMP
)
USING DELTA
LOCATION "/mnt/datalake/Output/YellowTaxis_Silver.delta"

PARTITIONED BY (PickupLocationId)

-- COMMAND ----------

-- MAGIC %md ####(A.3) Create Gold table

-- COMMAND ----------


CREATE TABLE TaxisDB.YellowTaxis_Gold
(
    PickupLocationId        INT,
    DropLocationId          INT,
    PickupYear              INT,
    PickupMonth             INT,
    PickupDay               INT,
    
    TotalRides              INT,
    TotalDistance           DOUBLE,    
    TotalAmount             DOUBLE    
)

USING DELTA

LOCATION "/mnt/datalake/Output/YellowTaxis_Gold.delta"

-- COMMAND ----------

-- MAGIC %md ###(B) Copy Data - Complete Load

-- COMMAND ----------

-- MAGIC %md ####(B.1) Copy data to Bronze table

-- COMMAND ----------


COPY INTO TaxisDB.YellowTaxis_Bronze
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
             
             , INPUT_FILE_NAME()     AS FileName
             , CURRENT_TIMESTAMP()   AS CreatedOn   
             
        FROM '/mnt/datalake/Raw/YellowTaxis/' 
    )
    
    FILEFORMAT = CSV    
    FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis_Bronze

-- COMMAND ----------

SELECT *

FROM table_changes('TaxisDB.YellowTaxis_Bronze', 1)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS

SELECT *
FROM table_changes('TaxisDB.YellowTaxis_Bronze', 1)

WHERE TotalAmount > 0  
  AND TripDistance > 0

-- COMMAND ----------

-- MAGIC %md ####(B.2) Incrementally move data to Silver table

-- COMMAND ----------

MERGE INTO TaxisDB.YellowTaxis_Silver tgt

    USING  BronzeChanges src               
    ON tgt.VendorId = src.VendorId 
    AND tgt.RideId = src.RideId
  
-- Update row if join conditions match
WHEN MATCHED
      THEN  
          UPDATE SET  tgt.PickupTime = src.PickupTime, 
          tgt.DropTime = src.DropTime, 
          tgt.PickupLocationId = src.PickupLocationId,
          tgt.DropLocationId = src.DropLocationId,
          tgt.CabNumber = src.CabNumber,
          tgt.DriverLicenseNumber = src.DriverLicenseNumber, 
          tgt.PassengerCount = src.PassengerCount, 
          tgt.TripDistance = src.TripDistance, 
          tgt.TotalAmount = src.TotalAmount,
          tgt.ModifiedOn = CURRENT_TIMESTAMP()
-- Insert row if row is not present in target table
WHEN NOT MATCHED
      THEN
          INSERT (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber,
                  PassengerCount, TripDistance, TotalAmount, CreatedOn, ModifiedOn)  
                  
          VALUES (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, 
                  PassengerCount, TripDistance, TotalAmount, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ####(B.3) Write data in Gold table

-- COMMAND ----------

INSERT OVERWRITE TABLE TaxisDB.YellowTaxis_Gold

    SELECT PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

         , COUNT(RideId)        AS TotalRides
         , SUM(TripDistance)    AS TotalDistance
         , SUM(TotalAmount)     AS TotalAmount

    FROM TaxisDB.YellowTaxis_Silver
    
    GROUP BY PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

-- COMMAND ----------

SELECT *
FROM TaxisDB.YellowTaxis_Bronze;

SELECT *
FROM TaxisDB.YellowTaxis_Silver;

SELECT *
FROM TaxisDB.YellowTaxis_Gold;

-- COMMAND ----------

-- MAGIC %md ###(C) Copy Data - Incremental Load

-- COMMAND ----------

-- MAGIC %md ####(C.1) Copy data from new file only to Bronze table

-- COMMAND ----------


COPY INTO TaxisDB.YellowTaxis_Bronze
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
             
             , INPUT_FILE_NAME()     AS FileName
             , CURRENT_TIMESTAMP()   AS CreatedOn
             
        FROM '/mnt/datalake/Raw/YellowTaxis/'
    )    
    
    FILEFORMAT = CSV    
    FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis_Bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS

SELECT *

FROM table_changes('TaxisDB.YellowTaxis_Bronze', 2)

WHERE TotalAmount > 0  
  AND TripDistance > 0

-- COMMAND ----------

-- MAGIC %md ####(C.2) Incrementally move data to Silver table

-- COMMAND ----------


MERGE INTO TaxisDB.YellowTaxis_Silver tgt

    USING  BronzeChanges src

        ON    tgt.VendorId  =  src.VendorId
          AND tgt.RideId    =  src.RideId
  
-- Update row if join conditions match
WHEN MATCHED
      THEN  
          UPDATE SET  tgt.PickupTime = src.PickupTime, tgt.DropTime = src.DropTime, tgt.PickupLocationId = src.PickupLocationId,
                      tgt.DropLocationId = src.DropLocationId, tgt.CabNumber = src.CabNumber,
                      tgt.DriverLicenseNumber = src.DriverLicenseNumber, tgt.PassengerCount = src.PassengerCount,
                      tgt.TripDistance = src.TripDistance, tgt.TotalAmount = src.TotalAmount,                     
                      
                      tgt.ModifiedOn = CURRENT_TIMESTAMP()

-- Insert row if row is not present in target table
WHEN NOT MATCHED

      THEN
          INSERT (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber,
                  PassengerCount, TripDistance, TotalAmount)          
          VALUES (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, 
                  PassengerCount, TripDistance, TotalAmount)

-- COMMAND ----------

-- MAGIC %md ####(C.3) Rewrite data in Gold table

-- COMMAND ----------


INSERT OVERWRITE TABLE TaxisDB.YellowTaxis_Gold

    SELECT PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

         , COUNT(RideId)        AS TotalRides
         , SUM(TripDistance)    AS TotalDistance
         , SUM(TotalAmount)     AS TotalAmount

    FROM TaxisDB.YellowTaxis_Silver
    
    GROUP BY PickupLocationId, DropLocationId, PickupYear, PickupMonth, PickupDay

-- COMMAND ----------

SELECT *
FROM TaxisDB.YellowTaxis_Gold

