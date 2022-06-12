// Databricks notebook source
val friends = List("Chandler", "Rachel", "Phoebe", "Joey", "Ross")

val friendsRDD = sc.parallelize(friends)

// COMMAND ----------

val ratings = sc.textFile("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/ratings.csv")

val count_ratings = ratings.count

// COMMAND ----------

val ratings = sc.textFile("hdfs://dev_server:9001/file.txt")

// COMMAND ----------

val letters = List('f', 'a', 'g', 'f', 'c', 'a', 'b', 'n', 'd', 'b')

val lettersRDD = sc.parallelize(letters)

// COMMAND ----------

lettersRDD.first 

lettersRDD.take(4) 

lettersRDD.collect 


// COMMAND ----------

val friends = List("Monica", "Chandler", "Ross", "Phoebe", "Rachel", "Joey") 

val friendsRDD = sc.parallelize(friends) 

val chandler = friendsRDD.filter(name=> name.contains("Chandler")) 

chandler.collect 


// COMMAND ----------

def find(name: List[String]): Boolean = {
	name.contains("Chandler")
	}



find(friends) 



// COMMAND ----------

val pairs = friendsRDD.map(name => (name.charAt(0), name)) 


pairs.foreach(println) 



// COMMAND ----------

val  num = List(1, 2, 3, 4)

val numRDD = sc.parallelize(num)




// COMMAND ----------

val squaredRDD = numRDD.map(x => x * x) 

squaredRDD.foreach(println) 

 

// COMMAND ----------

numRDD.map(x => x * x) 


def square(x: Int): Int = {
	x * x
}
numRDD.map(square)




// COMMAND ----------

val sumRDD = numRDD.reduce((a, b) => (a + b)) 


val mulRDD = numRDD.reduce((a, b) => (a * b)) 


// COMMAND ----------


