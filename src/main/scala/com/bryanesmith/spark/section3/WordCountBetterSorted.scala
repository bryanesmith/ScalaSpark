package com.bryanesmith.spark.section3

import org.apache.spark._
import org.apache.log4j._
import com.bryanesmith.spark.SparkContextExtras._

/**
  * Count up how many of each word occurs in a book, using regular expressions and sorting the final results
  */
object WordCountBetterSorted {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    sc.resourceTextFile("/section3/book.txt")
      .flatMap { _.split("\\W+") }
      .map { _.toLowerCase() }
      .map { (_, 1) }                 // [ (word, 1) ]
      .reduceByKey { _ + _ }          // [ (word, count) ]
      .map { x => (x._2, x._1) }      // [ (count, word) ]
      .sortByKey()                    // sorted by count
      .foreach { e =>
        println(s"${e._2}: ${e._1}")
      }

  } // main
  
} // WordCountBetterSorted

