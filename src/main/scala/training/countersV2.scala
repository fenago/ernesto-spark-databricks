package training

import org.apache.spark.sql.SparkSession

object countersV2 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Bad record counter V2")
      .getOrCreate()

    val badRecords = sparkSession.sparkContext.longAccumulator("Bad Records")

    import sparkSession.implicits._

    val options = Map("header" -> "false", "inferSchema" -> "true")
    val data = sparkSession.read.text("/home/jovyan/work/ernesto-spark/Files/chapter_6/ratings-malformed.csv").as[String]

    data.foreach(record => {
      val fields = record.split(",")

      if (fields.size != 4)
        badRecords.add(1)
    })

    println("The number of bad records in the input are " + badRecords.value)
    
    sparkSession.stop()
  }

}
