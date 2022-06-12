package training

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object counters {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Counters")
    val data = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_6/ratings-malformed.csv")

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
	
	sc.stop()

  }
}

