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
      .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/movies-head.csv")

    val ratings = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings-head.csv")

    movies.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")

    val joinedDf = spark
      .sql("SELECT * FROM movies JOIN ratings ON movies.movieId = ratings.movieId")

    joinedDf.show()
	
	spark.stop()
  }

}
