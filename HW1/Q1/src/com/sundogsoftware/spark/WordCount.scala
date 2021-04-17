package com.sundogsoftware.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object WordCount {
  
def main(args: Array[String]) {
Logger.getLogger("org").setLevel(Level.ERROR)
val startTimeMillis = System.currentTimeMillis() // start to save time

val conf = new SparkConf().setAppName("WordCount")
conf.setMaster("local[*]")    // if we change * to 1-4 number of cores are changed
val sc = new SparkContext(conf)

val input = sc.textFile("Shakespeare.txt")
val Totalwords = input.flatMap(line => line.split(' '))
val lowerCaseWords = Totalwords.map(word => word.toLowerCase())
.map(_.replaceAll("[^a-zA-Z]", ""))
val WorthWords= lowerCaseWords.filter(word => word.length > 0)
val wordArray= WorthWords.map(word => (word, 1))
println("\nTotal words (with repetition): " + wordArray.count())
val words  =wordArray.reduceByKey(_+_)
.sortBy(_._2, ascending = false)
words.collect()

println("Total words (without repetition): " + words.count() )

val results = words.filter(x => x._2 == 1)  //print number of Words that used just one time in .txt
System.out.println("Number of Words that used just one time in .txt : " + results.count())

println("\nMost repeated words : \n") //print most repeated words
words .take(10).foreach(println)

// compute run time
val endTimeMillis = System.currentTimeMillis()
val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
println("\nExecution Time : " + durationSeconds)
}}
