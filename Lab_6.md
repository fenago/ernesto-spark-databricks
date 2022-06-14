<img align="right" src="./logo-small.png">

# Lab : Apache Spark WordCount

#### Pre-reqs:
- Google Chrome (Recommended)

#### Lab Environment
All packages have been installed. There is no requirement for any setup.


## WordCount Example

**Step 1:** Now that we have editor to start writing the Spark code, the first thing we need to do is to include the import statements from Spark libraries. This program requires the following imports.

```
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
```

The first two import statements are used to import the Spark packages and last import statement is used to set logging level for our Spark application.


**Step 2:** Let us declare the level of logging to only log the error and fatal messages.

```
Logger.getLogger("Org").setLevel(Level.ERROR)
```

This step is not mandatory and you may skip this if you want all the logs.

**Step 3:** Let us now create a SparkContext object so that we can access all the spark functionality.

```
val sparkSession = SparkSession.builder
  .master("local[*]")
  .appName("WordCount")
  .getOrCreate()

val sc = sparkSession.sparkContext
```

We are creating an immutable variable called sc which cotains the SparkContext object. Inside the SparkContext object the first parameter tells Spark if we would want the program to be executed in local or distributed mode. In our case, since we are working locally, we will be using local[*]. The [*] tells Spark to use all the CPU cores available locally in our machine. The next parameter is just the name of our app which is WordCount.

**Step 4:** We now have a SparkContext object created. We can now use this object and load data using the textFile API as we have done in the Spark Shell. 
 

We already have cloned a github repository which contains a text file `treasure_island.txt`. Open `treasure_island.txt` to view text file. Write the following line of code to load the file to create an RDD.

```
val data = sc.textFile("chapter_4/treasure_island.txt")
```

With this we have successfully created an RDD using the text file.

This completes the process of creating a SparkContext object and creating the first RDD by loading the data using the textFile API.

Task is complete!

## Spark Program – Performing Operations

In the previous task, we have successfully created an RDD. Now let us use the RDD and apply operations to achieve our goal of counting number of words in a file.

**Step 1:** We have an RDD which contains text in lines. Let us split the lines to words using the flatMap function. The flatMap function is used to remove a level of nesting. The flatMap function is basically a combination map function and flatten function where the map function is applied first and then flatten function is applied.

```
val words = data.flatMap(line => line.split(" "))
```

The above piece of code splits each line into a seperate word. The logic we apply to split the line is by a white space character. The flatMap function takes the data RDD and splits each line of word by a space character.

**Step 2:** At this point, we have each word in a row. In order to count the occurences of each word, we have to map each word to a key-value pair where the key is the word itself and the value will be number 1.

```
val wordskv = words.map(word => (word, 1))
```
 

Here we use a map function to create a key value pair for each word where the key is the word and value is the literal number 1. With this operation we will end up having a tuples of word and the literal number 1.

**Step 3:** Now that we have tuples, all we need to do is add the values (literal number 1) for the same key. To achieve this, we use the reduceByKey function. As the name suggests, the reduceByKey function takes a key and applies operations to the values of that key.

```
val count = wordskv.reduceByKey((x,y) => x + y)
```


The above line of code takes the wordskv RDD and applied reduceByKey function to perform a sum of all the values for a key. This way we will end up with tuple where the first element is the word and the second element is the number of occurences for that word.


The reduceByKey function is similar to reduce function which we have seen in the previous chapter. The difference is that the reduceByKey function performs reduce operation on values for a given key in a tuple while reduce function is applied for all the elements in the collection. 

 **Step 4:** With the previous step all the transformations have been completed. Let us now perform an action to print out the result to console.

```
count.collect.foreach(println)
```

We can now simply use collect to collect the final RDD which is count and use foreach with println to print out each record in the RDD to the console. This will actually trigger the program to evaluate. All the trasnformations before this action are only logged in the Lineage graph to achieve lazy evaluation.

 

This completes our first ever Spark program. All we need to do now is to run it.

## Lab Solution

Lab solution is present in `wordCount.scala` file.

The execution might take some time based upon the hardware configuration of your machine.

![](./Screenshots/Chapter_4/Selection_018.png)

To view the result, scroll up the console until you see text in white as shown in the screenshot. This is the output result showing each word in the text file with number of it occurences.

## Spark Program – Saving Data

The output displayed in the console is useful while developing but in the real time we would want the output to be saved. 

**Step 1:** We shall be using the saveAsTextFile API to save the output of our program to the disk. Simply replace the collect statement from the previous task with this line of code.

```
count.saveAsTextFile("chapter_4/word_count/output")
```

**Important:** You need to uncomment above line in `wordCount.scala` using editor before running program again.

We shall be saving the output to the following path IdeaProjects/Spark/chapter_4/word_count/output. You need not create the directories word_count and output. They will be automatically created. In fact the compiler will throw an error if the output directory is already present.


## Spark Program – Lineage Graph

Let us now check the Lineage Graph for our Word Count program.

**Step 1:** To check the Lineage Graph for our Word Count program, we should use the toDebugString method. To do so, simply replace the saveAsTextFile line from previous task with the code below.

```
count.toDebugString.foreach(print)
```


Please note that we have used print inside foreach and not println.

![](./Screenshots/Chapter_4/Selection_023.png)

As you can see from the screenshot above, the `toDebugString` method displays the Lineage Graph. The indentations in the last four lines specify the shuffle boundary. Meaning, there was no shuffle of data for these opeartions: map, flatmap, teftFile. While the reduceByKey operation involves shuffling of data.


The number inside the paranthesis is to denote the number of parallel tasks. In our case it is only 1. The higher number denotes a high level of parallelism.

Task is complete!

