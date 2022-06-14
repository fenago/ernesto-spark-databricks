import org.apache.spark.sql.SparkSession


val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Bad record counter V2")
  .getOrCreate()

val badRecords = sparkSession.sparkContext.longAccumulator("Bad Records")

import sparkSession.implicits._

val options = Map("header" -> "false", "inferSchema" -> "true")
val data = sparkSession.read.text("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings_malformed.csv").as[String]

data.foreach(record => {
  val fields = record.split(",")

  if (fields.size != 4)
    badRecords.add(1)
})

println("The number of bad records in the input are " + badRecords.value)


