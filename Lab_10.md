<img align="right" src="./logo-small.png">

# Lab : Apache Spark Paired RDD Advanced

#### Pre-reqs:
- Google Chrome (Recommended)

#### Lab Environment
All packages have been installed. There is no requirement for any setup.


#### Lab Solution
Open https://github.com/fenago/ernesto-spark-databricks/tree/master/src/main/scala/training/tags.scala to view scala file.




The aim of the following lab exercises is to start writing Spark code in editor to learn about Paired RDDs.
We will cover following topics in this scenario.
- Creating a Paired RDD
- Performing Operations on Paired RDD


## Paired RDD

Let us now look at advance operations that we can perform on Paired RDDs.

**Step 1:** Download the tags.csv file from the URL below. This file contains four columns: userId, movieID, tag and timestamp.

tags.csv - http://bit.ly/2YTVGFk

**Note:** We already have cloned a github repository which contains a required file. Open `~/work/ernesto-spark/Files/chapter_5` to view file.

**Step 2:** Click **File Browser** tab on the top left and open `~/work/ernesto-spark/src/main/scala/training/tags.scala to view scala file.

```
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
```

**Step 3:** Write the recordsParser function as in the previous task. For this task, let us extract the movieID and tag fields. The recordsParser function is as shown below.

```
def parseRecords (rows: String): (Int, String)={
val records = rows.split(",")
val movieID = records(1).toInt
val tags = records(2).toString
(movieID, tags)
}
```


**Step 4:** Create a paired RDD as in Task 2 by writing the main function, setting error log level (optional), creating a SparkContext object and loading the file using the textFile API.

```
Logger.getLogger("Org").setLevel(Level.ERROR)
val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("Paired RDD Operations")
  .getOrCreate()
val sc = sparkSession.sparkContext

val data = sc.textFile("chapter_5/tags.csv")
```

Now create an RDD pair by parsing the data RDD using the recordsParser function.

```
val RDDPair = data.map(parseRecords)
```

We now have our paired RDD. Let us use some operations in the next step on our paired RDD in the next step.

**Step 5:** Now that we have our paired RDD, let us group all the tags by movieID using the groupByKey function.

```
val grouped = RDDPair.groupByKey()
```
 

Let us now print out the result of grouped RDD to the console.

```
grouped.collect.foreach(println)
```

The output is as shown in the screenshot below with all the tags for a movie are grouped together.

![](./Screenshots/Chapter_5/Selection_031.png)

**Step 5:** To run this program, run the following scala file code in the databricks notebook. The program will the then be compiled and executed.

`tags.scala` 
 

You may optionally convert the values from compactBuffer to a list by simply mapping the output and converting them to a List as shown below.

**Step 6:** We can also extract the keys and values to separate RDDs as shown below.

```
val RDDKeys = flattened.keys

RDDKeys.collect.foreach(println)
```

![](./Screenshots/Chapter_5/Selection_033.png)

Similarly, we can extract the values using the code below.

```
val RDDValues = flattened.values

RDDValues.collect.foreach(println)
```

**Important:** You need to uncomment above line in `tags.scala` using editor before running program again.

![](./Screenshots/Chapter_5/Selection_034.png)


`tags.scala` 

Task is complete!

