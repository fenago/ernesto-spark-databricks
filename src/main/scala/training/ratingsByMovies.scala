package training

import org.apache.spark.SparkContext
import scala.io.Source

object ratingsByMovies {

  def loadMovieNames(): Map[Int, String] = {

    var movieNames: Map[Int, String] = Map()

    val data = Source.fromFile("/home/jovyan/work/ernesto-spark/Files/chapter_6/movies.csv").getLines()

    for(record <- data){
      val fields = record.split(",")
      if(fields.length > 1)
        movieNames += (fields(0).replaceAll("\\uFEFF", "").toInt -> fields(1))
    }
    movieNames
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "Ratings By movies")

    val broadNames = sc.broadcast(loadMovieNames)

    val data = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_6/ratings.csv")
    val records = data.map(x => (x.split(",")(1).toInt, 1))
    val count = records.reduceByKey((x,y) => x + y)
    val sorted = count.sortBy(-_._2)

    val sortedMoviesWithNames = sorted.map(x => (broadNames.value(x._1), x._2))

    sortedMoviesWithNames.collect.foreach(println)

    sc.stop()
  }

}
