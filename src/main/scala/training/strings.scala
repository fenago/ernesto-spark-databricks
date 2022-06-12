package training

import org.apache.spark.sql.SparkSession

object strings {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Functions")
      .master("local[*]")
      .getOrCreate()

    val quote = List("I have no special talent.",
      "I am only passionately curious.",
      "I have a dream.",
      "I came, I saw, I conquered.")

    import spark.implicits._

    val quoteDS = quote.toDS().cache()
    quoteDS.show()

    import org.apache.spark.sql.functions._

    val splitted = quoteDS.select(split($"value", " ").as("splits"))
    splitted.show()

    val exploded = splitted.select(explode($"splits").as("explode"))
    exploded.show()

    val strLen = exploded.select($"explode", length($"explode")).as("length")
    strLen.show()

    val upCase = quoteDS.select(upper($"value").as("upperCase"))
    val lowCase = upCase.select(lower($"upperCase").as("lowerCase"))
    upCase.show()
    lowCase.show()

    val sub = quoteDS.select(substring($"value", 0, 2).as("firstWord"))
    val trimmed = sub.select(trim($"firstWord"))
    sub.show()
    trimmed.show()

  }

}
