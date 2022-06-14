import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


  def parseRecords(rows: String): (Int, Float) = {

    val records = rows.split(',')
    val userId = records(0).toInt
    val ratings = records(2).toFloat
    (userId, ratings)
  }


  Logger.getLogger("Org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Average ratings by users")
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings.csv")
  val RDDPair = data.map(parseRecords)

  RDDPair.collect.foreach(println)

  val mappedRatings = RDDPair.mapValues(x => (x, 1))
  //mappedRatings.collect.foreach(println)

  val totalRatings = mappedRatings.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))

  val avgRatings = totalRatings.mapValues(x => x._1/x._2)
  //avgRatings.collect.foreach(println)

  val sorted = avgRatings.sortByKey(false)

  sorted.collect.foreach(println)

