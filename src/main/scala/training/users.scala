package training

import org.apache.spark.sql.SparkSession

object users {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Users")
      .master("local[*]")
      .getOrCreate()

	val users = spark.read
	  .format("csv")
	  .options(Map("inferSchema" -> "true", "header" -> "true"))
	  .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/us-500.csv")

    users.printSchema()

    users.show()
	
	//users.select("last_name").show()

	//users.select("first_name", "last_name").show()
	
	spark.stop()

  }

}
