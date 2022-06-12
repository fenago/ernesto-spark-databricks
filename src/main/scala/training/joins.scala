package training

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel

object joins {

  case class ratings(userId: Int, movieID: Int, rating: Float, timestamp: String)
  case class movies(movieID: Int, movieName: String, genre:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Joins")
    val rating = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_5/ratings.csv").map(x => x.split(','))
    val movie = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_5/movies.csv").map(x => x.split(','))

    val rating_record = rating.map(x => (x(1).toInt, ratings(x(0).toInt, x(1).toInt,
      x(2).toFloat, x(3).toString)))

    val movie_record = movie.map(x => (x(0).toInt, movies(x(0).toInt, x(1).toString,
      x(2).toString)))

    val joined = rating_record.join(movie_record)
    joined.collect.foreach(println)
    
    joined.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val count = joined.countByKey()
    println(count)

    val mappedCol = joined.collectAsMap()
    println(mappedCol)

    val look = joined.lookup(25)
    println(look)

    }

  }
