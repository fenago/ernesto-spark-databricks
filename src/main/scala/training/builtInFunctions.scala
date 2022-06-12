package training

import org.apache.spark.sql.SparkSession

private case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: String)

object builtInFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Built-In functions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val movies = spark
      .read
      .format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_8/ratings-head.csv")
      .as[Rating]
      .cache()

    import org.apache.spark.sql.functions._

    val agg = movies.select(
        avg("rating")
      , min("userId")
      , max("movieId")
      , sum("userId")
      , mean("rating")
    )
    agg.show()

    val aggAlias = movies.select(
      avg("rating").as("avgRating")
      , min("userId").as("lowestUserId")
      , max("movieId").as("highestMovieId")
      , sum("userId").as("sumOfUserId")
      , mean("rating").as("meanRating")
    )
    aggAlias.show()

    val byUser = movies.groupBy("userId")
      .agg(countDistinct("rating").as("distinctCount")
        , sumDistinct("rating").as("distinctSum")
        , count("movieId").as("movieCount"))
    byUser.show()
	
	spark.stop()

  }

}
