package training

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object tags {

  def parseRecords(rows: String): (Int, String) = {

    val records = rows.split(',')
    val movieID = records(1).toInt
    val tags = records(2).toString
    (movieID, tags)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Paired RDD Operations")
    val data = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_5/tags.csv")
    val RDDPair = data.map(parseRecords)
    val grouped = RDDPair.groupByKey()
    grouped.collect.foreach(println)
	
    val flattened = grouped.map(x => (x._1, x._2.toList))
    //flattened.collect.foreach(println)
	
    val RDDValues = flattened.values
    //RDDValues.collect.foreach(println)
	
	sc.stop()
  }
}
