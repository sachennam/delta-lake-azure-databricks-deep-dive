-- Databricks notebook source
-- MAGIC %md ###(A) Create Live Bronze Table

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_BronzeLive
(
    RideId                  INT              COMMENT 'This is the primary key column',
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

LOCATION "/mnt/datalake/Output/YellowTaxis_BronzeLive.delta"

PARTITIONED BY (VendorId)

COMMENT "Live Bronze table for YellowTaxis"

AS

SELECT *

     , INPUT_FILE_NAME()     AS FileName
     , CURRENT_TIMESTAMP()   AS CreatedOn

FROM parquet.`/mnt/datalake/Raw/YellowTaxisParquet`

-- COMMAND ----------

-- MAGIC %md ###(B) Create Live Silver Table

-- COMMAND ----------


CREATE LIVE TABLE YellowTaxis_SilverLive
(
    RideId                  INT               COMMENT 'This is the primary key column',
    VendorId                INT,
    PickupTime              TIMESTAMP,
    DropTime                TIMESTAMP,
    PickupLocationId        INT,
    DropLocationId          INT,    
    TripDistance            DOUBLE,    
    TotalAmount             DOUBLE,
    
    PickupYear              INT              GENERATED ALWAYS AS (YEAR  (PickupTime)),
    PickupMonth             INT              GENERATED ALWAYS AS (MONTH (PickupTime)),
    PickupDay               INT              GENERATED ALWAYS AS (DAY   (PickupTime)),
        
    CreatedOn               TIMESTAMP,    
    
    -- Define the constraints
    CONSTRAINT Valid_TotalAmount    EXPECT (TotalAmount IS NOT NULL AND TotalAmount > 0) ON VIOLATION DROP ROW,
    
    CONSTRAINT Valid_TripDistance   EXPECT (TripDistance > 0)                            ON VIOLATION DROP ROW,
    
    CONSTRAINT Valid_RideId         EXPECT (RideId IS NOT NULL AND RideId > 0)           ON VIOLATION FAIL UPDATE
)

USING DELTA

LOCATION "/mnt/datalake/Output/YellowTaxis_SilverLive.delta"

PARTITIONED BY (PickupLocationId)

AS

SELECT RideId
     , VendorId
     , PickupTime
     , DropTime
     , PickupLocationId
     , DropLocationId     
     , TripDistance
     , TotalAmount
     
     , CURRENT_TIMESTAMP()   AS CreatedOn

FROM live.YellowTaxis_BronzeLive

-- COMMAND ----------

-- MAGIC %md ###(C) Create Live Gold Table - 1

-- COMMAND ----------


CREATE LIVE TABLE YellowTaxis_SummaryByLocation_GoldLive

LOCATION "/mnt/datalake/Output/YellowTaxis_SummaryByLocation_GoldLive.delta"

AS

SELECT PickupLocationId, DropLocationId

       , COUNT(RideId)        AS TotalRides
       , SUM(TripDistance)    AS TotalDistance
       , SUM(TotalAmount)     AS TotalAmount

FROM live.YellowTaxis_SilverLive
    
GROUP BY PickupLocationId, DropLocationId

-- COMMAND ----------

-- MAGIC %md ###(D) Create Live Gold Table - 2

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SummaryByDate_GoldLive

LOCATION "/mnt/datalake/Output/YellowTaxis_SummaryByDate_GoldLive.delta"

AS

SELECT PickupYear, PickupMonth, PickupDay

     , COUNT(RideId)        AS TotalRides
     , SUM(TripDistance)    AS TotalDistance
     , SUM(TotalAmount)     AS TotalAmount

FROM live.YellowTaxis_SilverLive

GROUP BY PickupYear, PickupMonth, PickupDay
