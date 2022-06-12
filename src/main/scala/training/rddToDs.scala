
package training

import org.apache.spark.sql.SparkSession

object rddToDs {

  case class Players(player_name: String, team: String, position: String, height: Int,
                     weight: Int, age: Double)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("Rdd to Dataset")
      .master("local[*]")
      .getOrCreate()

    val input = ss.sparkContext.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_8/mlb_players.csv")

    val removeHeader = input.mapPartitionsWithIndex((index, itr) => {
      if (index == 0) itr.drop(1) else itr
    })

    val fields = removeHeader.map(record => record.split(","))

    val structRecords = fields.map(field => Players(field(0).trim, field(1).trim, field(2).trim,
      field(3).trim.toInt, field(4).trim.toInt, field(5).trim.toDouble))

    import ss.implicits._

    val recordsDs = structRecords.toDS()

    recordsDs.show()

	ss.stop()
  }

}
