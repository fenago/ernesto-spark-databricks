package training

import org.apache.spark.sql.SparkSession

object math {

  def main (args: Array[String]):Unit = {

    val spark = SparkSession
      .builder()
      .appName("Math Functions")
      .master("local[*]")
      .getOrCreate()

    val numbers = List(5, 4, 9.4, 25, 8, 7.7, 6, 52)

    import spark.implicits._

    val numbersDS = numbers.toDS().withColumnRenamed("value", "numbers").cache()

    import org.apache.spark.sql.functions._


    val mathFuncs1 = numbersDS.select(abs($"numbers"), ceil($"numbers"), exp($"numbers"), cos($"numbers"))
    mathFuncs1.show()

    val mathFuncs2 = numbersDS.select(factorial($"numbers"), floor($"numbers"), hex($"numbers"), log($"numbers"))
    mathFuncs2.show()

    val mathFuncs3 = numbersDS.select(pow($"numbers", 2), round($"numbers"), sin($"numbers"), tan($"numbers"))
    mathFuncs3.show()

    val mathFuncs4 = numbersDS.select(sqrt($"numbers"), log10($"numbers"), $"numbers" + Math.PI)
    mathFuncs4.show()

  }

}
