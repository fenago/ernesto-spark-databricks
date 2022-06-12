package training

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object avgRatings {

  def parseRecords(rows: String): (Int, Float) = {

    val records = rows.split(',')
    val userId = records(0).toInt
    val ratings = records(2).toFloat
    (userId, ratings)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Average ratings by users")
    val data = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_5/ratings.csv")
    val RDDPair = data.map(parseRecords)

    RDDPair.collect.foreach(println)
    
    val mappedRatings = RDDPair.mapValues(x => (x, 1))
    //mappedRatings.collect.foreach(println)
    
    val totalRatings = mappedRatings.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))
    
    val avgRatings = totalRatings.mapValues(x => x._1/x._2)
    //avgRatings.collect.foreach(println)
    
    val sorted = avgRatings.sortByKey(false)

    sorted.collect.foreach(println)
	
	  sc.stop()

  }

}
