package training

import org.apache.spark.sql.SparkSession

object dfOps {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DataFrame Operations")
      .master("local[*]")
      .getOrCreate()

    val users = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_7/us-500.csv")

    /*val floridaUsers = users.select("*").where("state = \"FL\"")
    floridaUsers.show()

    val nyUserCount = users.groupBy("state")
      .agg(("state", "count"))
      .where("state = \"NY\"")
    nyUserCount.show()*/

    import spark.implicits._

    val userCountByState = users.groupBy("state")
      .agg(("state", "count"))
      .orderBy($"count(state)".desc)

    userCountByState.show()
	
	spark.stop()

  }

}
