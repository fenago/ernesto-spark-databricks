package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object createDf {

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

    val structRecords = fields.map(field => Row(field(0).trim, field(1).trim, field(2).trim,
      field(3).trim.toInt, field(4).trim.toInt, field(5).trim.toDouble))

    val schema = StructType(List(
      StructField("player_name", StringType, false),
      StructField("team", StringType, false),
      StructField("position", StringType, false),
      StructField("height", IntegerType, false),
      StructField("weight", IntegerType, false),
      StructField("age", DoubleType, false)
    ))

    val recordsDf = ss.sqlContext.createDataFrame(structRecords, schema)

    recordsDf.show()
	
	ss.stop()

  }

}
