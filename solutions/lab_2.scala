// Databricks notebook source
val num: Int = 255

// COMMAND ----------

val longNum: Long = 89416414

val decimal: Double = 85.5545

val decimalf: Float = 54.24f

val letter: Char = 'f'

// COMMAND ----------

val lieDetector: Boolean = true

val num = 256

// COMMAND ----------

val name = "Learning Voyage"

val decimal = 25.3545

// COMMAND ----------

println("Printing to console using concatenation: " + name + num + longNum + decimal + decimalf + letter + lieDetector)

// COMMAND ----------

println(s"Printing to console using variable substitution: $name $num $longNum $decimal $decimalf $letter $lieDetector")

// COMMAND ----------

println(s"Four divided by two is ${4/2}")

// COMMAND ----------

printf(f"Printing the value of a double with 2 decimal places $decimal%.2f")

// COMMAND ----------

val numOfKids = 3

if (numOfKids > 2) println ("They are Phoebe Buffay's kids.") else println ("Parent unknown!")

// COMMAND ----------

val  numOfKids = 3
if (numOfKids > 2) {
println("They are Phoebe Buffay's kids.")
} else {
println("Parent unknown!")
}

// COMMAND ----------

for ( i <- 1 until 5) {
	val sum = i + i
	println(sum)
}

// COMMAND ----------

val  friends = List("Chandler", "Monica", "Rachel", "Ross", "Joey", "Phoebe")
for(friend <- friends if friend == "Chandler"){
println(s"The king of sarcasm is $friend")
}

// COMMAND ----------

var friends = 0
val names = List("Chandler", "Monica", "Rachel", "Phoebe", "Ross", "Joey")
println("The names of friends are:")

while (friends < 6){
println(s"${names(friends)}")
friends += 1
}

// COMMAND ----------

var i = 0
do{
i += 1
println(i)
} while (i < 5)

// COMMAND ----------


