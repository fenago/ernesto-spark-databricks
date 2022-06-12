package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object decrRatingUDF {

  val decrUDF = udf((input: Double) => input - 0.5)

  def decrUDF2(input: Double): Double = {

    input - 0.5
  }

  case class Ratings(userId: Int, movieID: Int, rating: Double, timeStamp: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Ratings Decrement UDF")
      .getOrCreate()

    import spark.implicits._

    val ratings = spark
      .read
      .format("csv")
      .options(Map("InferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_9/ratings_head.csv")
      .as[Ratings]

    //Applying the UDF using DataFrame API

    val ratingDecDs = ratings.select($"*", decrUDF($"rating").as("ratingDec"))
    ratingDecDs.show()

    //Applying the UDF using Spark SQL API

    spark.udf.register("decrUDF2", decrUDF2 _)

    ratings.createOrReplaceTempView("ratings")

    val ratingDecDf = spark.sql("select *, decrUDF2(rating) as ratingDec from ratings")
    //ratingDecDf.show()
	
	spark.stop()

  }
}
