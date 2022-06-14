

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession



object recordParser {

  case class records(userIt: Int, movieId: Int, rating: Double, timeStamp: String)

  def parse(record:String): Either[String, records] = {

    val fields: Array[String] = record.split(',')

    if (fields.length == 4) {

      val userId: Int = fields(0).toInt
      val movieId: Int = fields(1).toInt
      val rating: Double = fields(2).toDouble
      val timeStamp: String = fields(3)

      Right(records(userId, movieId, rating, timeStamp))
    }

    else {
      Left(record)
    }

  }
}


Logger.getLogger("Org").setLevel(Level.ERROR)

val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Counters")
  .getOrCreate()
val sc = sparkSession.sparkContext

val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings_malformed.csv")

var badRecords = 0

data.foreach(row => {

  val parsedRecords = recordParser.parse(row)

  if (parsedRecords.isLeft)
    badRecords += 1

  else{
    val goodRecords = parsedRecords.right.map(x => (x.userIt, x.movieId, x.rating,
      x.timeStamp))
    goodRecords.foreach(println)
  }
})

println("The number of bad records in the input are " + badRecords)



