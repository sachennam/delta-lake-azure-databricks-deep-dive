# Databricks notebook source
# MAGIC %md ###(A) Setup environment for multiple processes

# COMMAND ----------

# MAGIC %md ####(A.1) Define Event Hub Namespace connection string

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Namespace Connection String
namespaceConnectionString = "Endpoint=sb://pstaxinamespaceeus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c+lyDdwpor9/wXE+NEPuw+njdMQC83ma8cvwhCWI/0w="

# Event Hub Name
eventHubName = "rideeventhub"

# Event Hub Connection String
eventHubConnectionString = namespaceConnectionString + ";EntityPath=" + eventHubName

# COMMAND ----------

# MAGIC %md ####(A.2) Define schema for Green Taxi Rides

# COMMAND ----------

rideSchema = (
                 StructType()
                    .add("RideId", "integer")
                    .add("VendorId", "integer")
                    .add("EventType", "string")
                    .add("PickupTime", "timestamp")
                    .add("PickupLocationId", "integer")
                    .add("CabLicense", "string")
                    .add("DriverLicense", "string")                    
                    .add("PassengerCount", "integer")
                    .add("RateCodeId", "integer")
                    .add("DropTime", "timestamp")
                    .add("DropLocationId", "integer")
                    .add("TripDistance", "double")
                    .add("PaymentType", "integer")
                    .add("TotalAmount", "double")
             )

# COMMAND ----------

# MAGIC %md ####(A.3) Cleanup files and re-create Green Taxis table (non-partitioned)

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/Output/GreenTaxis.delta", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS TaxisDB.GreenTaxis;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TaxisDB.GreenTaxis
# MAGIC (
# MAGIC    RideId             INT,
# MAGIC    VendorId           INT,
# MAGIC    EventType          STRING,
# MAGIC    PickupTime         TIMESTAMP,
# MAGIC    PickupLocationId   INT,
# MAGIC    CabLicense         STRING,
# MAGIC    DriverLicense      STRING,   
# MAGIC    PassengerCount     INT,   
# MAGIC    DropTime           TIMESTAMP,
# MAGIC    DropLocationId     INT,
# MAGIC    RateCodeId         INT,
# MAGIC    PaymentType        INT,
# MAGIC    TripDistance       DOUBLE,   
# MAGIC    TotalAmount        DOUBLE
# MAGIC )
# MAGIC 
# MAGIC USING DELTA
# MAGIC 
# MAGIC LOCATION "/mnt/datalake/Output/GreenTaxis.delta"

# COMMAND ----------

# MAGIC %md ###(B) Prepare Process 1: Only works with Vendor Id - 1
# MAGIC 
# MAGIC Steps:
# MAGIC 
# MAGIC 1. Prepare Event Hub configuration
# MAGIC 2. Read stream from event hub
# MAGIC 3. Extract ride information from Event Hub
# MAGIC 4. Apply filter for VendorId = 1

# COMMAND ----------

# Event Hub Configuration for Reader1
eventHubReader1 = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString),
  
  'eventhubs.consumerGroup' : 'reader1'
}

# Read stream from Event Hub
rideInputDF1 = (
                   spark
                      .readStream
                      .format("eventhubs")
                      .options(**eventHubReader1)
                      .load()
              )

# Extract rides information from Event Hub
rideDF1 = (
                  rideInputDF1
                      .select( from_json(col("body").cast("string"), rideSchema).alias("taxidata") )
  
                      .select("taxidata.RideId", "taxidata.VendorId", "taxidata.EventType", "taxidata.PickupTime", "taxidata.CabLicense",
                              "taxidata.DriverLicense", "taxidata.PickupLocationId", "taxidata.PassengerCount", "taxidata.RateCodeId",
                              "taxidata.DropTime", "taxidata.DropLocationId", "taxidata.TripDistance", "taxidata.PaymentType",
                              "taxidata.TotalAmount")
          )

# Filter records where VendorId = 1
rideDF1 = (
                  rideDF1  
                      .where("VendorId = 1")
          )

# COMMAND ----------

# MAGIC %md ###(C) Prepare Process 2: Works with Vendor Ids other than 1
# MAGIC 
# MAGIC Steps:
# MAGIC 
# MAGIC 1. Prepare Event Hub configuration
# MAGIC 2. Read stream from event hub
# MAGIC 3. Extract ride information from Event Hub
# MAGIC 4. Apply filter for VendorId <> 1

# COMMAND ----------

# Event Hub Configuration for Reader1
eventHubReader2 = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString),
  
  'eventhubs.consumerGroup' : 'reader2'
}

# Read stream from Event Hub
rideInputDF2 = (
                   spark
                      .readStream
                      .format("eventhubs")
                      .options(**eventHubReader2)
                      .load()
              )

# Extract rides information from Event Hub
rideDF2 = (
                  rideInputDF2
                      .select( from_json(col("body").cast("string"), rideSchema).alias("taxidata") )
  
                      .select("taxidata.RideId", "taxidata.VendorId", "taxidata.EventType", "taxidata.PickupTime", "taxidata.CabLicense",
                              "taxidata.DriverLicense", "taxidata.PickupLocationId", "taxidata.PassengerCount", "taxidata.RateCodeId",
                              "taxidata.DropTime", "taxidata.DropLocationId", "taxidata.TripDistance", "taxidata.PaymentType",
                              "taxidata.TotalAmount")
          )

# Filter records where VendorId <> 1
rideDF2 = (
                  rideDF2  
                       .where("VendorId <> 1")
          )

# COMMAND ----------

# MAGIC %md ###(D) Scenario 1: Simultaneous Inserts
# MAGIC 
# MAGIC Both processes will write to the same table at the same time

# COMMAND ----------

(
    rideDF1
        .writeStream
        .format("delta")
        .outputMode("append")  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisInsert1")
        .table("TaxisDB.GreenTaxis")
)

# COMMAND ----------

(
    rideDF2
        .writeStream
        .format("delta")
        .outputMode("append")  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisInsert2")
        .table("TaxisDB.GreenTaxis")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT VendorId
# MAGIC     , COUNT(*) 
# MAGIC FROM TaxisDB.GreenTaxis
# MAGIC GROUP BY VendorId
# MAGIC ORDER BY VendorId

# COMMAND ----------

# MAGIC %md ###(E) Scenario 2: Simultaneous Updates
# MAGIC 
# MAGIC Both processes will update to the same table (non-partitioned) at the same time

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE TaxisDB.GreenTaxis

# COMMAND ----------

def writeGreenTaxiEvents1(batchDF, batchId): 
  
  batchDF.createOrReplaceTempView("changes")
  
  (
     batchDF._jdf.sparkSession().sql("""
     
    MERGE INTO TaxisDB.GreenTaxis tgt
        USING changes src               ON tgt.RideId = src.RideId AND tgt.VendorId = 1   -- FOR VENDOR ID 1

      -- When conditions match, update record
      WHEN MATCHED AND src.EventType = 'P'
          THEN UPDATE SET tgt.PickupTime = src.PickupTime, tgt.CabLicense = src.CabLicense, tgt.DriverLicense = src.DriverLicense, 
                         tgt.PickupLocationId = src.PickupLocationId, tgt.PassengerCount = src.PassengerCount, tgt.RateCodeId = src.RateCodeId

      WHEN MATCHED AND src.EventType = 'D'
          THEN UPDATE SET tgt.DropTime = src.DropTime, tgt.DropLocationId = src.DropLocationId, tgt.TripDistance = src.TripDistance,
                          tgt.TotalAmount = src.TotalAmount, tgt.PaymentType = src.PaymentType

      -- When conditions does not match, insert record
      WHEN NOT MATCHED 
          THEN INSERT (RideId, VendorId, PickupTime, CabLicense, DriverLicense, PickupLocationId, PassengerCount, DropTime, DropLocationId, 
                       TripDistance, TotalAmount, RateCodeId, PaymentType) 
                       
               VALUES (RideId, VendorId, PickupTime, CabLicense, DriverLicense, PickupLocationId, PassengerCount, DropTime, DropLocationId, 
                       TripDistance, TotalAmount, RateCodeId, PaymentType) 
                  """)
  )  
  pass

# COMMAND ----------

def writeGreenTaxiEvents2(batchDF, batchId): 
  
  batchDF.createOrReplaceTempView("changes")
  
  (
     batchDF._jdf.sparkSession().sql("""
     
    MERGE INTO TaxisDB.GreenTaxis tgt
        USING changes src               ON tgt.RideId = src.RideId AND tgt.VendorId <> 1     -- FOR VENDOR IDs OTHER THAN 1           

      -- When conditions match, update record
      WHEN MATCHED AND src.EventType = 'P'
          THEN UPDATE SET tgt.PickupTime = src.PickupTime, tgt.CabLicense = src.CabLicense, tgt.DriverLicense = src.DriverLicense, 
                         tgt.PickupLocationId = src.PickupLocationId, tgt.PassengerCount = src.PassengerCount, tgt.RateCodeId = src.RateCodeId
                                    
      WHEN MATCHED AND src.EventType = 'D'
          THEN UPDATE SET tgt.DropTime = src.DropTime, tgt.DropLocationId = src.DropLocationId, tgt.TripDistance = src.TripDistance,
                          tgt.TotalAmount = src.TotalAmount, tgt.PaymentType = src.PaymentType

      -- When conditions does not match, insert record
      WHEN NOT MATCHED 
          THEN INSERT (RideId, VendorId, PickupTime, CabLicense, DriverLicense, PickupLocationId, PassengerCount, DropTime, DropLocationId, 
                       TripDistance, TotalAmount, RateCodeId, PaymentType) 
                       
               VALUES (RideId, VendorId, PickupTime, CabLicense, DriverLicense, PickupLocationId, PassengerCount, DropTime, DropLocationId, 
                       TripDistance, TotalAmount, RateCodeId, PaymentType)                
        """)
  )  
  pass

# COMMAND ----------

(
    rideDF1
        .writeStream
  
        .foreachBatch (writeGreenTaxiEvents1)
  
        .outputMode("append")
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate1")  
        .trigger(processingTime = '5 seconds')
        .start()
)

# COMMAND ----------

(
    rideDF2
        .writeStream
  
        .foreachBatch (writeGreenTaxiEvents2)
  
        .outputMode("append")
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate2")  
        .trigger(processingTime = '5 seconds')
        .start()
)

# COMMAND ----------

# MAGIC %md ###(F) Scenario 3: Simultaneous Updates (in different partitions)
# MAGIC 
# MAGIC Both processes will update to the same table at the same time. One for VendorId=1, and second one for all other vendors.

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/Output/GreenTaxis.delta", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS TaxisDB.GreenTaxis;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TaxisDB.GreenTaxis
# MAGIC (
# MAGIC    RideId             INT,
# MAGIC    VendorId           INT,
# MAGIC    EventType          STRING,
# MAGIC    PickupTime         TIMESTAMP,
# MAGIC    PickupLocationId   INT,
# MAGIC    CabLicense         STRING,
# MAGIC    DriverLicense      STRING,   
# MAGIC    PassengerCount     INT,   
# MAGIC    DropTime           TIMESTAMP,
# MAGIC    DropLocationId     INT,
# MAGIC    RateCodeId         INT,
# MAGIC    PaymentType        INT,
# MAGIC    TripDistance       DOUBLE,   
# MAGIC    TotalAmount        DOUBLE
# MAGIC )
# MAGIC 
# MAGIC USING DELTA
# MAGIC 
# MAGIC PARTITIONED BY (VendorId)
# MAGIC 
# MAGIC LOCATION "/mnt/datalake/Output/GreenTaxis.delta"

# COMMAND ----------

(
    rideDF1
        .writeStream
        .foreachBatch (writeGreenTaxiEvents1)
        .outputMode("append")
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate1_Partitioned")  
        .trigger(processingTime = '5 seconds')
        .start()
)

# COMMAND ----------

(
    rideDF2
        .writeStream
        .foreachBatch (writeGreenTaxiEvents2)
        .outputMode("append")
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate2_Partitioned")  
        .trigger(processingTime = '5 seconds')
        .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM TaxisDB.GreenTaxis
# MAGIC WHERE PickupTime IS NOT NULL AND DropTime IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Not in video
# MAGIC 
# MAGIC DESCRIBE HISTORY TaxisDB.GreenTaxis
