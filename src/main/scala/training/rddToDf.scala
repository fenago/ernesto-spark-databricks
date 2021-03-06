import org.apache.spark.sql.SparkSession

case class Players(player_name: String, team: String, position: String, height: Int,
                 weight: Int, age: Double)


val ss = SparkSession
  .builder()
  .appName("Rdd to DataFrame")
  .master("local[*]")
  .getOrCreate()

val input = ss.sparkContext.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/mlb_players.csv")

val header = input.first()
val records = input.filter(x => x != header)

val fields = records.map(record => record.split(","))

val structRecords = fields.map(field => Players(field(0).trim, field(1).trim, field(2).trim,
  field(3).trim.toInt, field(4).trim.toInt, field(5).trim.toDouble))

import ss.implicits._

val recordsDf = structRecords.toDF()

recordsDf.show()

