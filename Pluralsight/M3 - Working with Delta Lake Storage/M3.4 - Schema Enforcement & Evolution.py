# Databricks notebook source
# MAGIC %md ###(A) Write to Parquet & Delta formats

# COMMAND ----------

# Extract records from Data Lake for RateCodes

rateCodes1DF = (
                     spark
                       .read
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv("/mnt/datalake/Raw/RateCodes/RateCodes1.csv")
               )

display(rateCodes1DF)

# COMMAND ----------

#Write in parquet format

(
    rateCodes1DF
        .write
        .mode("overwrite")
        
        .format("parquet")
  
        .save("/mnt/datalake/Output/RateCodes.parquet")
)

# COMMAND ----------

#Write in delta format

(
    rateCodes1DF
        .write
        .mode("overwrite")
        
        .format("delta")
  
        .save("/mnt/datalake/Output/RateCodes.delta")
)

# COMMAND ----------

# MAGIC %md ###(B) Append data to Parquet format
# MAGIC 
# MAGIC The new file to be appended has additional columns

# COMMAND ----------

# Extract records from Data Lake for RateCodes2

rateCodes2DF = (
                     spark
                       .read
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv("/mnt/datalake/Raw/RateCodes/RateCodes2.csv")
               )

display(rateCodes2DF)

# COMMAND ----------

#Append in parquet format

(
    rateCodes2DF
        .write
        .mode("append")
        
        .format("parquet")
  
        .save("/mnt/datalake/Output/RateCodes.parquet")
)

# COMMAND ----------

# Extract records from output folder of RateCodes

rateCodesDF = (
                     spark
                       .read
                       .format("parquet")
                       .load("/mnt/datalake/Output/RateCodes.parquet")
              )

display(rateCodesDF)

# COMMAND ----------

# MAGIC %md ###(C) Append data to Delta format
# MAGIC 
# MAGIC The new file to be appended has additional columns

# COMMAND ----------

#Append in delta format

(
    rateCodes2DF
        .write
        .mode("append")
        
        .format("delta")
  
        .save("/mnt/datalake/Output/RateCodes.delta")
)

# COMMAND ----------

# MAGIC %md ###(D) Merge Changes in Delta Format
# MAGIC 
# MAGIC Set the property 'mergeSchema' to true

# COMMAND ----------

#Append in delta format

(
    rateCodes2DF
        .write
        .mode("append")
        
        .format("delta")
  
        .option("mergeSchema", "true")
  
        .save("/mnt/datalake/Output/RateCodes.delta")
)

# COMMAND ----------

# Extract records from output folder of RateCodes

rateCodesDF = (
                     spark
                       .read
                       .format("delta")
                       .load("/mnt/datalake/Output/RateCodes.delta")
              )

display(rateCodesDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Not in video
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM parquet.`/mnt/mstrainingdatalake/taxisoutput/RateCodes.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Not in video
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/mnt/mstrainingdatalake/taxisoutput/RateCodes.delta`
