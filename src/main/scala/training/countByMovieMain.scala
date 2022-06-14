import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.HashMap

class CountByMovie extends AccumulatorV2[(Int, Int), HashMap[Int,Int]]{

  private val movieCount = new HashMap[Int, Int]()

  def reset(): Unit = {
    movieCount.clear()
  }

  def add(tuple: (Int,Int)): Unit = {
    val movieId = tuple._1
    val updatedCount = tuple._2 + movieCount.getOrElse(movieId, 0)

    movieCount += ((movieId, updatedCount))
  }

  def merge(tuples: AccumulatorV2[(Int, Int), HashMap[Int, Int]]): Unit = {
    tuples.value.foreach(add)
  }

  def value() = movieCount

  def isZero(): Boolean = {
    movieCount.isEmpty
  }

  def copy() = new CountByMovie

}



import org.apache.spark.sql.SparkSession

case class Movies(userId: Int, movieId: Int, rating: Double, timeStamp: String)


val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Count By movieId")
  .getOrCreate()

val countByMovie = new CountByMovie()
sparkSession.sparkContext.register(countByMovie)

import sparkSession.implicits._

val options = Map("header" -> "true", "inferSchema" -> "true")
val data = sparkSession.read.format("com.databricks.spark.csv")
    .options(options)
    .load("dbfs:/FileStore/shared_uploads/ather.tahir786@outlook.com/ratings_head.csv")
    .as[Movies]

data.foreach(record => {
  countByMovie.add(record.movieId, 1)
})

println(countByMovie.value.toList)

