<img align="right" src="./logo-small.png">

# Lab : Apache Spark File Formats - Parquet and ORC

#### Pre-reqs:
- Google Chrome (Recommended)

#### Lab Environment
All packages have been installed. There is no requirement for any setup.





#### Lab Solution
Open https://github.com/fenago/ernesto-spark-databricks/Files/chapter_10` to view files.

![](./Screenshots/files.png)

The aim of the following lab exercises is to read and write various file formats in Spark applications.
We will cover following topics in this scenario.
- Parquet Files
- ORC Files


#### Install pyspark

**Note:** Spark is already installed. It is not required to run following command to install

PySpark is available in pypi. To install just run `pip install pyspark` 

## Parquet Files

Parquet is a widely used file format in Spark because of the optimizations it provides for analytical processing and is considered as one of the most efficient file formats for Spark. Parquet is column-oriented data structure and is the default file format in Spark. When no format is specified, Spark automatically process them as Parquet. Parquet is compressable, splittable but is not human readable. Parquet supports complex data structures. It can be easily processed when columns are of struct, map or array type. This is not possible with JSON and CSV files.


Parquet has only a couple of options while reading and writing the data. The following is an example to read a Parquet file.

```
spark.read
.format("parquet")
.load("/usr/local/files/sample.parquet")
```

We can also read without specifying the schema as shown below.

```
spark.read
.load("/usr/local/files/sample.parquet")
```

## Task: Parquet Files

Parquet is Spark's default file format. Let us read and write Parquet files in Spark.

**Step 1:** Download the file userdata1.parquet from the URL below and save it to the /home/jovyan/work/ernesto-spark/Files/chapter_10 folder.

userdata1.parquet - http://bit.ly/2kfIhJ4

**Note:** We already have cloned a github repository which contains a required file. Open `~/work/ernesto-spark/Files/chapter_10` to view file.


**Step 2:** Let us no read this Parquet file to Spark using the code below.

Open the terminal and fire up the Spark shell `spark-shell`.

Execute the following code in the notebook.

 ```
val  parquetData = spark
.read
.load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/userdata1.parquet")
```

Please see that we need not mention the format here as Parquet is default file format in Spark. However, you may explicitly mention the format as we did in the previous tasks if you desire so.

`parquetData.show()` 

You should see the following output when you call the show method on the dataframe.

![](./Screenshots/Chapter_10/Selection_015.png)


**Step 3:** Let us write this back to the filesystem in Parquet format.

`parquetData.write.save("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/output4")`
 
#### Output
We can check if the save was successful by simply running the cat command from a new terminal as shown below. However, you will not be able to read the file correctly as it is not human readable.


`cat dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/part*`

Run above command in **terminal 2**. You can also open New terminal by Clicking `File` > `New` > `Terminal` from the top menu.


**Step 4:** We can also save a parquet file using compression as shown below.

`parquetData.write.option("codec", "gzip").save("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/output5")`

Task is complete!

## ORC Files
 
ORC is yet another columnar file format which stands for Optiimized Row columnar. There are no options for ORC files because Spark processes ORC file format very efficiently. Both ORC and Parquet are similar but Parquet is very much optimized for Spark and ORC is optimized for Hive. 

ORC supports complex data structures, is splittable and can be compressed. ORC is not human readable. The following shows an example of reading an ORC file.

```
spark.read
.format("orc")
.load("/usr/local/files/sample.orc")
```

## Task: ORC Files

**Step 1:** Download the file userdata1_orc from the URL below and save it to the /home/jovyan/work/ernesto-spark/Files/chapter_10 folder.

userdata1.orc - http://bit.ly/2kfQi0J

**Note:** We already have cloned a github repository which contains a required file. Open `~/work/ernesto-spark/Files/chapter_10` to view file.

**Step 2:** Reading an ORC file is similar to what we have been doing so far through out this exercise.

Execute the following code in the notebook.

```
val orcData = spark
.read
.format("orc")
.load("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/userdata1_orc")
```


 `orcData.show()` 

You should see the following output when you call the show method on the dataframe.

![](./Screenshots/Chapter_10/Selection_017.png)



**Step 3:** We can now simply write to an ORC format similar to what we have been doing with other file formats so far.

`orcData.write.format("orc").save("dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/output6")`


#### Output
Similar to Parquet, ORC is also not human readable and you will only see gibberish data when used the cat command as shown below.

`cat dbfs:/FileStore/shared_uploads/UPDATE_PATH_HERE/part*`

Run above command in **terminal 2**. You can also open New terminal by Clicking `File` > `New` > `Terminal` from the top menu.


Task is complete!

























