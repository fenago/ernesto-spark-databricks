package training

import org.apache.spark.sql.SparkSession

object sqlQueries {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Users")
      .master("local[*]")
      .getOrCreate()

    val users = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_7/us-500.csv")

    users.createOrReplaceTempView("users")

    val floridaUsers = spark.sql("SELECT * FROM users WHERE state = \"FL\"")
    floridaUsers.show()

    /*val totalUsersNJ = spark.sql("SELECT count(*) AS NJ_Count FROM users WHERE state = \"NJ\"")
    totalUsersNJ.show()*/

    spark.conf.set("spark.sql.shuffle.partitions", "1")

    
	  /*val userCountByState = spark.sql("SELECT state, count(*) AS count FROM users" +
      " GROUP BY state ORDER BY count DESC")
    userCountByState.show()*/

    /*userCountByState.write
      .format("csv")
      .save("/home/jovyan/work/ernesto-spark/Files/chapter_7/output")*/
	  
	spark.stop()

  }

}
