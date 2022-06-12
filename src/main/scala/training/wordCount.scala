package training

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object wordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("Org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")
    val data = sc.textFile("/home/jovyan/work/ernesto-spark/Files/chapter_4/treasure_island.txt")
    val words = data.flatMap(lines => lines.split(" "))
    val wordskv = words.map(word => (word,1))
    val count = wordskv.reduceByKey((x,y) => x + y)

    count.collect.foreach(println)

    //count.saveAsTextFile("chapter_4/word_count/output")

    //count.toDebugString.foreach(print)

    sc.stop()

  }

}
