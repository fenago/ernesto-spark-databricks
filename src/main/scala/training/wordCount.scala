import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


Logger.getLogger("Org").setLevel(Level.ERROR)
val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("WordCount")
  .getOrCreate()
val sc = sparkSession.sparkContext
val data = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/treasure_island.txt")
val words = data.flatMap(lines => lines.split(" "))
val wordskv = words.map(word => (word,1))
val count = wordskv.reduceByKey((x,y) => x + y)

count.collect.foreach(println)

//count.saveAsTextFile("chapter_4/word_count/output")

//count.toDebugString.foreach(print)
