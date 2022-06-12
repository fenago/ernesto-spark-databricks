package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dateTime {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DateTime_Functions")
      .master("local[*]")
      .getOrCreate()

    val dates = Seq(
      (1, "Ernesto", "2015-09-24"),
      (2, "Lee", "1985-05-16"),
      (3, "John", "2012-07-16"),
      (4, "Doe", "1914-08-02")
    )

    import spark.implicits._

    val datesDS = dates.toDS()
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "date")
    datesDS.printSchema()

    val casted = datesDS
      .select($"id", $"name", $"date".cast("date"))
      .cache()
    casted.printSchema()
    casted.show()

    val extracted = casted
      .withColumn("year", year($"date"))
      .withColumn("month", month($"date"))
      .withColumn("dayOfYear", dayofyear($"date"))
      .withColumn("quarter", quarter($"date"))
      .withColumn("weekOfYear", weekofyear($"date"))
    extracted.show()

    val arithmetic = casted
      .withColumn("ageInDays", datediff(current_date(), $"date"))
      .withColumn("addedDays", date_add($"date", 25))
      .withColumn("subtrDays", date_sub($"date", 16))
      .withColumn("addedMonths", add_months($"date", 4))
      .withColumn("lastDay", last_day($"date"))
      .withColumn("nextDay", next_day($"date", "tuesday"))
      .withColumn("monthsBetween", months_between(current_date(), $"date", true))
    arithmetic.show()

    val timeStamp = spark.createDataset(Seq(
      (1, "Ernesto", "2015-09-24 00:01:12"),
      (2, "Lee", "1985-05-16 03:04:15"),
      (3, "John", "2012-07-16 06:07:18"),
      (4, "Doe", "1914-08-02 09:10:20")
    ))

    val timeStampDS = timeStamp
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "timeStamp")
    timeStampDS.printSchema()

    val castedTimeStamp = timeStampDS.select($"id", $"name", $"timeStamp".cast("timestamp")).cache()
    castedTimeStamp.printSchema()
    castedTimeStamp.show()

    val extractedTs = timeStampDS
      .withColumn("second", second($"timeStamp"))
      .withColumn("minute", minute($"timeStamp"))
      .withColumn("hour", hour($"timeStamp"))
    extractedTs.show()

    val conversions = timeStampDS.withColumn("unixTime", unix_timestamp($"timeStamp")).withColumn("fromUnix", from_unixtime($"unixTime"))
    conversions.show()

  }

}
