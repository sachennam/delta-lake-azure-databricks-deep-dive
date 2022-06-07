-- Databricks notebook source
-- MAGIC %md ###(A) Create & load Delta Table (non-optimized)

-- COMMAND ----------

CREATE TABLE TaxisDB.YellowTaxis_NonOptimized
(
    RideId                  INT               COMMENT 'This is the primary key column',
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
    ImprovementSurcharge    DOUBLE
)

USING DELTA

LOCATION "/mnt/datalake/Output/YellowTaxis_NonOptimized.delta"

PARTITIONED BY (PickupLocationId)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC 
-- MAGIC # Create schema for Green Taxi Data
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
-- MAGIC 
-- MAGIC yellowTaxisDF = (
-- MAGIC                     spark
-- MAGIC                         .read
-- MAGIC                         .option("header", "true")
-- MAGIC                         .schema(yellowTaxiSchema)
-- MAGIC                         .csv("/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv")
-- MAGIC                 )

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC 
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write  
-- MAGIC         .mode("append")  
-- MAGIC         .partitionBy("PickupLocationId")  
-- MAGIC         .format("delta")           
-- MAGIC         .save("/mnt/datalake/Output/YellowTaxis_NonOptimized.delta")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md ###(B) Create Delta Table with Auto-optimization enabled

-- COMMAND ----------


CREATE TABLE TaxisDB.YellowTaxis_Optimized
(
    RideId                  INT               COMMENT 'This is the primary key column',
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
    ImprovementSurcharge    DOUBLE
)
USING DELTA
LOCATION "/mnt/datalake/Output/YellowTaxis_Optimized.delta"

PARTITIONED BY (PickupLocationId)

TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write  
-- MAGIC         .mode("append")  
-- MAGIC         .partitionBy("PickupLocationId")  
-- MAGIC         .format("delta")           
-- MAGIC         .save("/mnt/datalake/Output/YellowTaxis_Optimized.delta")
-- MAGIC )

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

