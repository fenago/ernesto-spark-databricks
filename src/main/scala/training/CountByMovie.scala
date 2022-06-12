package training

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
