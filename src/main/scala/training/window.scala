package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object window {

  case class Employee(name: String, number: Int, dept: String, pay: Double, manager: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Window Functions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employeeDS = spark
      .read
      .format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_8/employee.csv")
      .as[Employee]

    val window = Window.partitionBy($"dept").orderBy($"pay".desc)

    val ranked = rank().over(window)
    val rankedDS = employeeDS.select($"*", ranked.as("rank"))
    val second = rankedDS.where("rank = 3")
    second.show()

    val denseRanked = dense_rank().over(window)
    employeeDS.select($"*", denseRanked.as("dense_rank")).show()

    val rowNumber = row_number().over(window)
    employeeDS.select($"*", rowNumber.as("rowNumber")).show()

    val percentRank = percent_rank().over(window)
    employeeDS.select($"*", percentRank.as("percentRank")).show()

    val leads = lead($"pay", 1, 0).over(window)
    employeeDS.select($"*", leads.as("lead")).show()

    val lags = lag($"pay", 1, 0).over(window)
    employeeDS.select($"*", lags.as("lag")).show()
	
	spark.stop()

  }

}
