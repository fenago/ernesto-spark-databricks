package training

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Ratings(userId: Int, movieID: Int, rating: Double, timestamp: String)
case class Average(var sum: Double, var count: Long)

object averageTypedUDAF extends Aggregator[Ratings, Average, Double] {

  def zero: Average = Average(0, 0L)

  def reduce(buffer: Average, rat: Ratings): Average = {
    buffer.sum += rat.rating
    buffer.count += 1
    buffer
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = reduction.sum / reduction.count

  def bufferEncoder: Encoder[Average] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}

object avgTypedUADF{

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Average ratings typed UDAF")
      .getOrCreate()

    import sparkSession.implicits._

    val ds = sparkSession.read
      .format("csv")
      .options(Map("InferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_9/ratings_head.csv")
      .as[Ratings]

    val averageRating = averageTypedUDAF.toColumn.name("averageRating")
    val avg = ds.select(averageRating)

    avg.show()
	
	sparkSession.stop()
  }

}
