import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder()
  .appName("SQL Joins")
  .master("local[*]")
  .getOrCreate()

val movies = spark.read
  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/movies_head.csv")

val ratings = spark.read
  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings_head.csv")

movies.createOrReplaceTempView("movies")
ratings.createOrReplaceTempView("ratings")

val joinedDf = spark
  .sql("SELECT * FROM movies JOIN ratings ON movies.movieId = ratings.movieId")

joinedDf.show()
