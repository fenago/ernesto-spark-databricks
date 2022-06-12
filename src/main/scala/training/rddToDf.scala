package training

import org.apache.spark.sql.SparkSession

object rddToDf {

  case class Players(player_name: String, team: String, position: String, height: Int,
                   weight: Int, age: Double)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("Rdd to DataFrame")
      .master("local[*]")
      .getOrCreate()

    val input = ss.sparkContext.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_7/mlb_players.csv")

    val header = input.first()
    val records = input.filter(x => x != header)

    val fields = records.map(record => record.split(","))

    val structRecords = fields.map(field => Players(field(0).trim, field(1).trim, field(2).trim,
      field(3).trim.toInt, field(4).trim.toInt, field(5).trim.toDouble))

    import ss.implicits._

    val recordsDf = structRecords.toDF()

    recordsDf.show()

    ss.stop()
  }

}
