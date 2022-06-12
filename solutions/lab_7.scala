// Databricks notebook source
val num = List(1, 2, 3, 4)

val numRDD = sc.parallelize(num)

// COMMAND ----------

val squaredRDD = numRDD.map(x => x * x)

squaredRDD.foreach(println)

// COMMAND ----------


