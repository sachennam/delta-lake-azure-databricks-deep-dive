-- Databricks notebook source
CREATE TABLE TaxisDB.PaymentTypes
(
    PaymentTypeId       INT,
    PaymentTypeName     STRING,
    IsEnabled           INT
)

USING DELTA

TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

INSERT INTO TaxisDB.PaymentTypes (PaymentTypeId, PaymentTypeName, IsEnabled)
VALUES (1, 'Credit Card', 1)

-- COMMAND ----------

INSERT INTO TaxisDB.PaymentTypes (PaymentTypeId, PaymentTypeName, IsEnabled)
VALUES (2, 'Cash', 1)

-- COMMAND ----------


DESCRIBE HISTORY TaxisDB.PaymentTypes

-- COMMAND ----------

SELECT *
FROM table_changes ('TaxisDB.PaymentTypes', 1)

-- COMMAND ----------

UPDATE TaxisDB.PaymentTypes
SET IsEnabled = 0
WHERE PaymentTypeId = 2


-- COMMAND ----------

-- Run delete statement

DELETE FROM TaxisDB.PaymentTypes
WHERE PaymentTypeId = 1

-- COMMAND ----------

SELECT *

FROM table_changes ('TaxisDB.PaymentTypes', 3, 4)

ORDER BY _commit_timestamp

