<img align="right" src="./logo-small.png">

# Lab : Apache Spark - Joins using Spark SQL

#### Pre-reqs:
- Google Chrome (Recommended)

#### Lab Environment
All packages have been installed. There is no requirement for any setup.

#### Lab Solution
Open https://github.com/fenago/ernesto-spark-databricks/tree/master/src/main/scala/training/sqlJoins.scala to view scala file.



The aim of the following lab exercises is to start writing Spark SQL code in editor to learn about Data Frames.
We will cover following topics in this scenario.
- Joins using Spark SQL
- Operations using DataFrame API




## Joins using Spark SQL

Let us now use Spark SQL to join two dataFrames.

**Step 1:** Download the ratings.csv file from the URL below. This file contains four columns: userId, movieID, rating and timestamp.

ratings-head.csv - http://bit.ly/2FPdhHE

**Note:** We already have cloned a github repository which contains a required file. Open `~/work/ernesto-spark/Files/chapter_7` to view file.

**Step 2:** Download the movies.csv file from the URL below. This file contains three columns: movieID, movieName and genre.

movies-head.csv - http://bit.ly/2RTg72N

**Note:** We already have cloned a github repository which contains a required file. Open `~/work/dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/` to view file.

We shall join these datasets based on the movieID.


**Step 3:** Click **File Browser** tab on the top left and open `~/work/ernesto-spark/src/main/scala/training/sqlJoins.scala to view scala file.

```
import org.apache.spark.sql.SparkSession
```

Then write our program and create a SparkSession object as shown below.

```
  val spark = SparkSession
    .builder()
    .appName("SQL Joins")
    .master("local[*]")
    .getOrCreate()
```

**Step 4:** Let us now read both the files as shown below.

```
val movies = spark.read
  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("chapter_7/movies-head.csv")
val ratings = spark.read

  .format("csv")
  .options(Map("inferSchema" -> "true", "header" -> "true"))
  .load("chapter_7/ratings-head.csv")
```

We now have two dataFrames for each of our input file.


**Step 4:** Now that we have our dataFrames, let us create a temp view so that we can run our join query against them.

```
movies.createOrReplaceTempView("movies")
ratings.createOrReplaceTempView("ratings")
```

**Step 5:** We now have our views. All we need to do now is to perform the join. We can join this using the SQL query as shown below.

```
val joinedDf = spark.sql("SELECT * FROM movies JOIN ratings ON movies.movieId = ratings.movieId"
```

Finally, let us call the show method on our joinedDf dataFrame and run the program.

```
joinedDf.show()
```


To run this program, run the following scala file code in the databricks notebook. The program will the then be compiled and executed.
`sqlJoins.scala` 

You should see the joined table as shown in the screenshot below.

Task is complete!

## Operations using DataFrame API

**Step 1:** The initial few steps are similar to what we have done in the previous tasks. Create a new Scala object and name it dfOps. Specify the required imports and create a SparkSession object as in the previous tasks. Finally load the us-500.csv file.

Your program at this point of time should look like the one in the screenshot below.

 
**Step 2:** Now that we have the dataFrame created, let us perform an operation to select all the users from Florida using the DataFrame API.

```
val foridaUsers = users.select("*").where("state = \"FL\"")
```

This is similar to the SQL query which we have performed in the task earlier. We have methods here which look more like programming style. In the code above, we have used select method to select all the columns of out dataFrame and then where method to specify our condition.

Please see that we need not create a temp view as we did earlier. It is only required when we are working with SQL queries.

Let us now call the show method on our dataFrame to view the results on the console.

```
floridaUsers.show()
```

The output should look something like the table shown in the screenshot.



**Step 3:** Let us now run a query to check the count of total users who belong to state "New York".
 
```
val nyUserCount = users.groupBy("state")
    .agg(("state", "count"))
    .where("state = \"NY\"")
```

This is a bit different to what we have done in the SQL query. In the code above, we are first grouping by state and then applying the agg method. The agg method takes the column as first parameter and then the type of aggregation as second parameter. We specify the second parameter as count since we want to count the number of users from New York State. Then, we specify the condition using the where method.

Let us now call the show method on our dataFrame to view the results.

```
nyUserCount.show()
```

The output is as shown in the screenshot below.

![](./Screenshots/Chapter_7/Selection_044.png)


**Step 4:** Let us now write some code to get the count for all the users for each state. We first need to import the implicits as shown below.

```
import spark.implicits._

val userCountByState = users.groupBy("state")
    .agg(("state", "count"))
    .orderBy($"count(state".desc)
```

As you can see in the query above, we use the orderBy method to order the result by the count of state in a descending order.

Let us call the show method.

```
userCountByState.show()
```

To run this program, run the following scala file code in the databricks notebook. The program will the then be compiled and executed.
`dfOps.scala` 

The output is as shown in the screenshot below.

![](./Screenshots/Chapter_7/Selection_047.png)
 
Task is complete!
 























