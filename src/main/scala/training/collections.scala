package training

import org.apache.spark.sql.SparkSession

object collections {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Collections")
      .master("local[*]")
      .getOrCreate()

    val num = Seq(Seq(1,2,3), Seq(4, 5, 6), Seq(7,8,9))

    import spark.implicits._

    val numDS = num.toDS().withColumnRenamed("value", "numbers").cache()

    import org.apache.spark.sql.functions._

    val contains = numDS.where(array_contains($"numbers", 5))
    contains.show()

    val exploded = numDS.select($"numbers", explode($"numbers").as("exploded"))
    exploded.show()

    val posExploded = numDS.select(posexplode($"numbers"))
    posExploded.show()

    val sizeDS = numDS.select($"numbers", size($"numbers").as("size"))
    sizeDS.show()

    val sorted = numDS.select($"numbers", sort_array($"numbers", false).as("sorted"))
    sorted.show()

	spark.stop()
	
  }

}
