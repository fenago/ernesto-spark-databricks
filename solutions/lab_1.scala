// Databricks notebook source
println("Hello World!")

val name: String = "Learning Voyage"
println(name)

// COMMAND ----------

name = name + " Inc"

// COMMAND ----------

var newName: String = "Learning"

newName = newName + " Voyage"

println(newName)

// COMMAND ----------

val name: String = "Learning"

val newName: String = name + " Voyage"

println(newName)

// COMMAND ----------

val welcome: Option[String] = Some("Welcome to Learning Voyage")

welcome.get



// COMMAND ----------

val welcome: Option[String] = None

welcome.getOrElse("Ernesto Lee Website")

// COMMAND ----------


