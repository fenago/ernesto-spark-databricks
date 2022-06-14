

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


Logger.getLogger("Org").setLevel(Level.ERROR)

val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Counters")
  .getOrCreate()
val sc = sparkSession.sparkContext

val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings-malformed.csv")

val badRecords = sc.accumulator(0, "bad records")

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

println("The number of bad records in the input are " + badRecords.value)



