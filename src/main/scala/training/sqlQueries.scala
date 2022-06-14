import org.apache.spark.sql.SparkSession


val spark = SparkSession
  .builder()
  .appName("Users")
  .master("local[*]")
  .getOrCreate()

val users = spark.read
  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/us_500.csv")

users.createOrReplaceTempView("users")

val floridaUsers = spark.sql("SELECT * FROM users WHERE state = \"FL\"")
floridaUsers.show()

/*val totalUsersNJ = spark.sql("SELECT count(*) AS NJ_Count FROM users WHERE state = \"NJ\"")
totalUsersNJ.show()*/

spark.conf.set("spark.sql.shuffle.partitions", "1")


/*val userCountByState = spark.sql("SELECT state, count(*) AS count FROM users" +
  " GROUP BY state ORDER BY count DESC")
userCountByState.show()*/

/* userCountByState.write
    .format("csv")
    .save("chapter_7/output") */

