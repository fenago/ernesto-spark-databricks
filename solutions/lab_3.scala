// Databricks notebook source
val  job = "Transponster"
job match {
case "Masseuse" => println("That's Phoebe")
case "Chef" => println("That's Monica")
case "Executive" => println("That's Rachel")
case "Transponster" => println("That's Chandler")
case "Actor" => println ("That's Joey")
case "Paleontologist" => println("That's Ross")
case _ => println("Unknown job role")
}

// COMMAND ----------

def hello = {
println("Hello there!")
}

// COMMAND ----------

def married(name: String, times: Int): String = {
return name + " has married " + times + " times"
}

// COMMAND ----------

married("Ross", 3)

// COMMAND ----------

def squared (num: Int) : Int = {
num * num
}

// COMMAND ----------

def highSquared(num: Int, func: Int => Int): Int = {
	func(num)
	}

// COMMAND ----------

val result = highSquared(4, squared)

println(result)

// COMMAND ----------

highSquared(6, x => x * x)

// COMMAND ----------

val shows: List[String] = List("F.R.I.E.N.D.S", "The Big Bang Theory", "Game of Thrones", "Breaking Bad", "The Mentalist") 


// COMMAND ----------

println(s"Some of the popular TV shows are: $shows") 

println(s"The TV show at position 0 is ${shows(0)}") 

println(s"The TV show at position 1 is ${shows(1)}") 

println(s"The TV show at position 4 is ${shows(4)}") 
 



// COMMAND ----------



println(shows.head) 

println(shows.tail) 



// COMMAND ----------

 
shows.foreach(println) 


shows.map(show => show.toLowerCase) 

// COMMAND ----------

val couples = Map("Chandler" -> "Monica", "Ross" -> "Rachel", "Phoebe" -> "Mike")


// COMMAND ----------

println(couples("Chandler"))

// COMMAND ----------

println(couples("Joey"))

// COMMAND ----------

val unknown = util.Try(couples("Joey")) getOrElse "Not Known"

println(unknown)

// COMMAND ----------

val showInfo = (1994, "Friends", 8.8, 2011, "Game Of Thrones", 9.4, 2010, "Sherlock", 9.1)

// COMMAND ----------

println(showInfo._1)

println(showInfo._5)

println(s"${showInfo._5} is the highest rated show with ${showInfo._6} rating.")

// COMMAND ----------


