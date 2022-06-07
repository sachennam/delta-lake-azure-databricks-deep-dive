# Databricks notebook source
# MAGIC %md ###(A) Setup environment

# COMMAND ----------

# Namespace Connection String
namespaceConnectionString = "Endpoint=sb://pstaxinamespaceeus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c+lyDdwpor9/wXE+NEPuw+njdMQC83ma8cvwhCWI/0w="

# Event Hub Name
eventHubName = "rideeventhub"

# Event Hub Connection String
eventHubConnectionString = namespaceConnectionString + ";EntityPath=" + eventHubName

# COMMAND ----------

# Event Hub Configuration for Reader1
eventHubReader1 = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString),
  
  'eventhubs.consumerGroup' : 'reader1'
}

# COMMAND ----------

# MAGIC %md ###(B) Read data from Event Hubs

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

rideInputDF = (
                   spark
                      .readStream
                      .format("eventhubs")
                      .options(**eventHubReader1)
                      .load()
              )

display(
      rideInputDF,
      streamName = "RidesInputMemoryQuery",
      processingTime = '5 seconds'  
)

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

rideDF = (
                  rideInputDF
                      .select(
                                from_json(
                                              col("body").cast("string"),
                                              rideSchema
                                         )
                                  .alias("taxidata")
                             )
  
                      .select(
                                "taxidata.RideId",
                                "taxidata.VendorId",
                                "taxidata.EventType",
                                "taxidata.PickupTime",
                                "taxidata.CabLicense",
                                "taxidata.DriverLicense",
                                "taxidata.PickupLocationId",
                                "taxidata.PassengerCount",
                                "taxidata.RateCodeId",
                                "taxidata.DropTime",
                                "taxidata.DropLocationId",
                                "taxidata.TripDistance",
                                "taxidata.PaymentType",
                                "taxidata.TotalAmount"
                             )
          )

# COMMAND ----------

display(
      rideDF,
      streamName = "RidesMemoryQuery",
      processingTime = '5 seconds'  
)

# COMMAND ----------

# MAGIC %md ###(C) Create table & write data for Green Taxis

# COMMAND ----------

# MAGIC %sql
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

(
    rideDF
        .writeStream

        .format("delta")

        .outputMode("append")
  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxis")
  
        .trigger(processingTime = '5 seconds')

        .table("TaxisDB.GreenTaxis")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM TaxisDB.GreenTaxis

# COMMAND ----------

# MAGIC %md ###(D) Create & write to a Green Taxis Summary table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TaxisDB.GreenTaxisSummary
# MAGIC (
# MAGIC    VendorId         INT,
# MAGIC    TotalRides       LONG
# MAGIC )
# MAGIC 
# MAGIC USING DELTA
# MAGIC 
# MAGIC LOCATION "/mnt/datalake/Output/GreenTaxisSummary.delta"

# COMMAND ----------

ridesSummaryDF = (
                      rideDF
                          .where("EventType='P'")

                          .groupBy("VendorId")
                          .agg(count("RideId").alias("TotalRides"))
                 )

(
    ridesSummaryDF  
        .writeStream
        .format("delta")

        .outputMode("complete")
  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisSummary")
  
        .trigger(processingTime = '5 seconds')
  
        .table("TaxisDB.GreenTaxisSummary")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM TaxisDB.GreenTaxisSummary

# COMMAND ----------

# MAGIC %md ###(E) Update Delta Table with Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM TaxisDB.GreenTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE TaxisDB.GreenTaxis

# COMMAND ----------

def writeGreenTaxiEvents(batchDF, batchId): 
  
    # MERGE data into Delta Table
  
  pass

# COMMAND ----------

(
    rideDF
        .writeStream

        .foreachBatch (writeGreenTaxiEvents)

        .outputMode("append")                      
  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate")
  
        .trigger(processingTime = '5 seconds')
          
        .start()
)

# COMMAND ----------

def writeGreenTaxiEvents(batchDF, batchId): 
  
  batchDF.createOrReplaceTempView("changes")
  
  (
     batchDF._jdf.sparkSession().sql("""
     
      MERGE INTO TaxisDB.GreenTaxis tgt
          USING changes src               ON tgt.RideId = src.RideId AND tgt.VendorId = src.VendorId               

        -- When conditions match, update record
        WHEN MATCHED AND src.EventType = 'P'
            THEN UPDATE SET tgt.PickupTime = src.PickupTime, tgt.CabLicense = src.CabLicense, tgt.DriverLicense = src.DriverLicense, 
                         tgt.PickupLocationId = src.PickupLocationId, tgt.PassengerCount = src.PassengerCount, tgt.RateCodeId = src.RateCodeId

        WHEN MATCHED AND src.EventType = 'D'
            THEN UPDATE SET tgt.DropTime = src.DropTime, tgt.DropLocationId = src.DropLocationId, tgt.TripDistance = src.TripDistance,         
                            tgt.TotalAmount = src.TotalAmount, tgt.PaymentType = src.PaymentType

        -- When conditions do not match, insert record
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
    rideDF
        .writeStream

        .foreachBatch (writeGreenTaxiEvents)

        .outputMode("append")                      
  
        .option("checkpointLocation", "/mnt/datalake/Output/Checkpoints/GreenTaxisUpdate")
  
        .trigger(processingTime = '5 seconds')
          
        .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM TaxisDB.GreenTaxis
# MAGIC ORDER BY RideId

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM TaxisDB.GreenTaxis
# MAGIC WHERE PickupTime IS NOT NULL AND DropTime IS NOT NULL
# MAGIC ORDER BY RideId

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY TaxisDB.GreenTaxis
