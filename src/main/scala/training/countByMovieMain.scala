package training

import org.apache.spark.sql.SparkSession

private case class Movies(userId: Int, movieId: Int, rating: Double, timeStamp: String)

object countByMovieMain {

  def main(args: Array[String]): Unit = {

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
        .load("/home/jovyan/work/ernesto-spark/Files/chapter_6/ratings_head.csv")
        .as[Movies]

    data.foreach(record => {
      countByMovie.add(record.movieId, 1)
    })

    println(countByMovie.value.toList)

    sparkSession.stop()
  }

}
