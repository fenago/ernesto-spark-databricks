

import org.apache.spark.sql.SparkSession

object dfOps {


    val spark = SparkSession
      .builder()
      .appName("DataFrame Operations")
      .master("local[*]")
      .getOrCreate()

    val users = spark.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/us-500.csv")

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
	


  }

}
