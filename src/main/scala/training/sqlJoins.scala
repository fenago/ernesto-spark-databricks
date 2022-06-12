package training

import org.apache.spark.sql.SparkSession

object sqlJoins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SQL Joins")
      .master("local[*]")
      .getOrCreate()

    val movies = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_7/movies-head.csv")

    val ratings = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_7/ratings-head.csv")

    movies.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")

    val joinedDf = spark
      .sql("SELECT * FROM movies JOIN ratings ON movies.movieId = ratings.movieId")

    joinedDf.show()
	
	spark.stop()
  }

}
