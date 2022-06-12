package training

import org.apache.spark.sql.expressions
.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object averageUDAF extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(Array(StructField("inputColumn", DoubleType)))

  def bufferSchema = StructType(Array(StructField("sum", DoubleType), StructField("count", LongType)))

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.00
    buffer(1) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getLong(1)

}

object avgRatingUDAF {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Average Rating UDAF")
      .getOrCreate()

    sparkSession.udf.register("averageUDAF", averageUDAF)

    val ratings = sparkSession.read
      .format("csv")
      .options(Map("InferSchema" -> "true", "header" -> "true"))
      .load("/home/jovyan/work/ernesto-spark/Files/chapter_9/ratings_head.csv")

    ratings.createOrReplaceTempView("ratings")

    val average = sparkSession.sql("SELECT userId, averageUDAF(rating) " +
      "AS avgRating FROM ratings " +
      "GROUP BY userId")

    average.show()
	
	sparkSession.stop()
  }
}