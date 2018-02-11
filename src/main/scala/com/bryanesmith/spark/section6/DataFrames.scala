package com.bryanesmith.spark.section6

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark.sql._
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.sparkContext.resourceTextFile("/section3/fakefriends.csv")
    val people = lines.map(mapper).toDS().cache()
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    people.select("name").show()
    
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    val byAge = people.groupBy("age").count().orderBy("age")
    byAge.printSchema()
    byAge.foreach { r =>
      println(r(0) + " " + r(1))
    }
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }
}