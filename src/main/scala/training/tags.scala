

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


def parseRecords(rows: String): (Int, String) = {

  val records = rows.split(',')
  val movieID = records(1).toInt
  val tags = records(2).toString
  (movieID, tags)
}

Logger.getLogger("Org").setLevel(Level.ERROR)
val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Paired RDD Operations")
  .getOrCreate()
val sc = sparkSession.sparkContext
val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/tags.csv")
val RDDPair = data.map(parseRecords)
val grouped = RDDPair.groupByKey()
grouped.collect.foreach(println)

val flattened = grouped.map(x => (x._1, x._2.toList))
//flattened.collect.foreach(println)

val RDDValues = flattened.values
//RDDValues.collect.foreach(println)
	
