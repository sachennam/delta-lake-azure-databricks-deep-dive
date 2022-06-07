-- Databricks notebook source
-- MAGIC %md ###(A) Read data from Data Lake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Create schema for Green Taxi Data
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC   
-- MAGIC yellowTaxiSchema = (
-- MAGIC                       StructType()
-- MAGIC   
-- MAGIC                         .add("RideId", "integer")
-- MAGIC   
-- MAGIC                         .add("VendorId", "integer")
-- MAGIC 
-- MAGIC                         .add("PickupTime", "timestamp")
-- MAGIC                         .add("DropTime", "timestamp")
-- MAGIC 
-- MAGIC                         .add("PickupLocationId", "integer")
-- MAGIC                         .add("DropLocationId", "integer")
-- MAGIC 
-- MAGIC                         .add("CabNumber", "string")
-- MAGIC                         .add("DriverLicenseNumber", "string")
-- MAGIC 
-- MAGIC                         .add("PassengerCount", "integer")
-- MAGIC                         .add("TripDistance", "double")
-- MAGIC   
-- MAGIC                         .add("RatecodeId", "integer")
-- MAGIC                         .add("PaymentType", "integer")
-- MAGIC 
-- MAGIC                         .add("TotalAmount", "double")  
-- MAGIC                         .add("FareAmount", "double")
-- MAGIC                         .add("Extra", "double")
-- MAGIC                         .add("MtaTax", "double")
-- MAGIC                         .add("TipAmount", "double")
-- MAGIC                         .add("TollsAmount", "double")                      
-- MAGIC                         .add("ImprovementSurcharge", "double")
-- MAGIC                    )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC yellowTaxisDF = (
-- MAGIC                     spark
-- MAGIC                       .read
-- MAGIC                       .option("header", "true")
-- MAGIC                       .schema(yellowTaxiSchema)
-- MAGIC                       .csv("/mnt/datalake/Raw/YellowTaxis/YellowTaxis1.csv")
-- MAGIC                 )
-- MAGIC 
-- MAGIC yellowTaxisDF.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(yellowTaxisDF)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md ###(B) Write data in Parquet format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write
-- MAGIC         .mode("overwrite")
-- MAGIC   
-- MAGIC         .partitionBy("VendorId")
-- MAGIC   
-- MAGIC         .format("parquet")
-- MAGIC   
-- MAGIC         .save("/mnt/datalake/Output/YellowTaxis.parquet")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md ###(C) Write data in Delta format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write
-- MAGIC         .mode("overwrite")
-- MAGIC   
-- MAGIC         .partitionBy("VendorId")
-- MAGIC   
-- MAGIC         .format("delta") 
-- MAGIC           
-- MAGIC         .save("/mnt/datalake/Output/YellowTaxis.delta")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md ###(D) Options to create Delta Tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS TaxisDB

-- COMMAND ----------

-- MAGIC %md ####Option 1: Reference Data Location in Data Lake
-- MAGIC 
-- MAGIC Just provide the path of data to register/create a table
-- MAGIC 
-- MAGIC <i>Note: You can also use: CREATE OR REPLACE TABLE / CREATE TABLE IF NOT EXISTS

-- COMMAND ----------

CREATE TABLE TaxisDB.YellowTaxisParquet

  USING PARQUET
  
  LOCATION "/mnt/datalake/Output/YellowTaxis.parquet"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS TaxisDB.YellowTaxis

  USING DELTA 
  
  LOCATION "/mnt/datalake/Output/YellowTaxis.delta"

-- COMMAND ----------

MSCK REPAIR TABLE TaxisDB.YellowTaxisParquet;

-- COMMAND ----------

SELECT COUNT(*)
FROM TaxisDB.YellowTaxisParquet

-- COMMAND ----------


SELECT COUNT(*)
FROM TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %md ###(E) 'Describe Table' commands
-- MAGIC 
-- MAGIC There are several describe commands to check details of a table
-- MAGIC 
-- MAGIC <i>Note: You can use: DESCRIBE TABLE / DESCRIBE TABLE EXTENDED / DESCRIBE DETAIL

-- COMMAND ----------

DESCRIBE TABLE EXTENDED TaxisDB.YellowTaxis

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

DROP TABLE TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.rm("/mnt/datalake/Output/YellowTaxis.delta", True)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC 
-- MAGIC #Save DataFrame as Delta Table
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write
-- MAGIC         .mode("overwrite")
-- MAGIC   
-- MAGIC         .partitionBy("VendorId")
-- MAGIC   
-- MAGIC         .format("delta")
-- MAGIC   
-- MAGIC         .option("path", "/mnt/datalake/Output/YellowTaxis.delta")
-- MAGIC   
-- MAGIC         .saveAsTable("TaxisDB.YellowTaxis")
-- MAGIC )

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------


CREATE TABLE TaxisDB.YellowTaxis
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
    ImprovementSurcharge    DOUBLE,
    
    PickupYear              INT              GENERATED ALWAYS AS (YEAR  (PickupTime))    COMMENT 'Auto-generated year from PickupTime',
    PickupMonth             INT              GENERATED ALWAYS AS (MONTH (PickupTime))    COMMENT 'Auto-generated month from PickupTime',
    PickupDay               INT              GENERATED ALWAYS AS (DAY   (PickupTime))    COMMENT 'Auto-generated day from PickupTime'
)

USING DELTA                  -- default in Databricks is Delta

LOCATION "/mnt/datalake/Output/YellowTaxis.delta"

PARTITIONED BY (VendorId)    -- optional

COMMENT 'This table stores ride information for Yellow Taxis'

-- COMMAND ----------

DESCRIBE TABLE EXTENDED TaxisDB.YellowTaxis
