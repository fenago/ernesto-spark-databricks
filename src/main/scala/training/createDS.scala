package training

import org.apache.spark.sql.SparkSession


object createDS {

  private case class Movies(userId: Int, movieId: Int, rating: Double, timestamp: String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Creating a Dataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val movies = spark
      .read
      .format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_8/ratings-head.csv")
      .as[Movies]
      .cache()

    movies.printSchema()

    movies.show()

    val ratingCount = movies.groupBy("rating").count()
    ratingCount.show()

    val users = movies.map(x => (x.userId, x.rating))
    val count = users.rdd.mapValues(x => (x,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1/x._2).sortByKey(false)

     count.collect.foreach(println)
    
    spark.stop()
  }

}
