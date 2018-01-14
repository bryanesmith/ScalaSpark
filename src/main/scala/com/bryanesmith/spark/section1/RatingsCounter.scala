package com.bryanesmith.spark.section1

import org.apache.log4j._
import org.apache.spark._

import scala.io.Source

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Format: userID, movieID, rating, timestamp
    def extractRating(line:String) = line.split("\t")(2)

    sc.textFile(getClass.getResource("/section1/ml-100k/u.data").getFile)
      .map(extractRating)
      .countByValue
      .toSeq
      .sortBy(_._1)
      .foreach(println)
  }
}
