

import org.apache.spark.sql.SparkSession

case class Players(first_name: String, last_name: String, company_name: String, address: String,
                    city: String, county: String, state: String, zip: Int, phone1: String,
                    phone2: String, web: String)


val ss = SparkSession
  .builder()
  .appName("Rdd to DataFrame")
  .master("local[*]")
  .getOrCreate()

val input = ss.sparkContext.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/us_500.csv")

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
