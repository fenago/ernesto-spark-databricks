// Databricks notebook source
val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/treasure_island.txt") 


// COMMAND ----------

data.take(5)
 
data.count()

// COMMAND ----------

val data2 = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/treasure_island.txt")


// COMMAND ----------

import org.apache.spark.storage.StorageLevel

data2.cache()

// COMMAND ----------

data2.take(5)


// COMMAND ----------

data2.count()


// COMMAND ----------

data2.unpersist()

// COMMAND ----------


