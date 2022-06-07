-- Databricks notebook source
-- MAGIC %md ###(A) Re-create Parquet & Delta tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.rm("/mnt/datalake/Output/YellowTaxis.parquet", True)
-- MAGIC dbutils.fs.rm("/mnt/datalake/Output/YellowTaxis.delta", True)

-- COMMAND ----------

DROP TABLE IF EXISTS TaxisDB.YellowTaxisParquet;
DROP TABLE IF EXISTS TaxisDB.YellowTaxis;

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
-- MAGIC 
-- MAGIC yellowTaxisDF = (
-- MAGIC                     yellowTaxisDF
-- MAGIC                         .withColumn("TripYear", year("PickupTime"))
-- MAGIC                         .withColumn("TripMonth", month("PickupTime"))
-- MAGIC                         .withColumn("TripDay", dayofmonth("PickupTime"))
-- MAGIC                 )

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python 
-- MAGIC 
-- MAGIC # Store data in Parquet format
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write
-- MAGIC         .mode("overwrite")
-- MAGIC   
-- MAGIC         .partitionBy("PickupLocationId")  
-- MAGIC   
-- MAGIC         .format("parquet") 
-- MAGIC   
-- MAGIC         .option("path", "/mnt/datalake/Output/YellowTaxis.parquet")
-- MAGIC         .saveAsTable("TaxisDB.YellowTaxisParquet")
-- MAGIC )
-- MAGIC 
-- MAGIC # Store data in Delta format
-- MAGIC (
-- MAGIC     yellowTaxisDF
-- MAGIC         .write
-- MAGIC         .mode("overwrite")
-- MAGIC   
-- MAGIC         .partitionBy("PickupLocationId")  
-- MAGIC   
-- MAGIC         .format("delta") 
-- MAGIC   
-- MAGIC         .option("path", "/mnt/datalake/Output/YellowTaxis.delta")
-- MAGIC         .saveAsTable("TaxisDB.YellowTaxis")
-- MAGIC )

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %md ###(B) Counting of Records
-- MAGIC </br>
-- MAGIC 
-- MAGIC - Parquet table scans each file to count the number of records
-- MAGIC - Delta table gets the count from transaction log

-- COMMAND ----------

SELECT COUNT(*)
FROM TaxisDB.YellowTaxisParquet

-- COMMAND ----------

SELECT COUNT(*)
FROM TaxisDB.YellowTaxis

-- COMMAND ----------

-- MAGIC %md ###(C) Data Skipping
-- MAGIC </br>
-- MAGIC 
-- MAGIC - Parquet table scans each file to get a record
-- MAGIC - Both Parquet and Delta tables support partition elimination

-- COMMAND ----------

SELECT *  
FROM TaxisDB.YellowTaxisParquet
WHERE RideId = 67

-- COMMAND ----------

SELECT *  
FROM TaxisDB.YellowTaxis
WHERE RideId = 67

-- COMMAND ----------


