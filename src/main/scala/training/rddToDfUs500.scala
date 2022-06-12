package training

import org.apache.spark.sql.SparkSession

object rddToDfUs500 {

  case class Players(first_name: String, last_name: String, company_name: String, address: String,
                     city: String, county: String, state: String, zip: Int, phone1: String,
                     phone2: String, web: String)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("Rdd to DataFrame")
      .master("local[*]")
      .getOrCreate()

    val input = ss.sparkContext.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_7/us-500.csv")

    val header = input.first()
    val records = input.filter(x => x != header)

    val fields = records.map(record => record
      .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))

    val structRecords = fields.map(field => Players(field(0).trim, field(1).trim, field(2).trim,
      field(3).trim, field(4).trim, field(5).trim, field(6).trim, field(7).trim.toInt, field(8).trim,
        field(9).trim, field(10).trim))

    import ss.implicits._

    val recordsDf = structRecords.toDF()

    recordsDf.show()

  }

}
