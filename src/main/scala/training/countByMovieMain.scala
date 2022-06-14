

import org.apache.spark.sql.SparkSession

private case class Movies(userId: Int, movieId: Int, rating: Double, timeStamp: String)

object countByMovieMain {


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
        .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings_head.csv")
        .as[Movies]

    data.foreach(record => {
      countByMovie.add(record.movieId, 1)
    })

    println(countByMovie.value.toList)

    
  }

}
