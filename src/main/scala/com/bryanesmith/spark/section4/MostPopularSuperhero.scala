package com.bryanesmith.spark.section4

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._

/**
  * Find the superhero with the most co-appearances.
  */
object MostPopularSuperhero {

  /**
    * Function to extract the hero ID and number of connections from each line
    */
  private def countCoOccurences(line: String): (Int, Int) = {
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }

  private def parseIdAndName(line: String) : Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else {
      None
    }
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperhero")   

    val namesRdd = sc.resourceTextFile("/section4/marvel-names.txt")
      .flatMap(parseIdAndName)
    
    val mostPopular = sc.resourceTextFile("/section4/marvel-graph.txt")
      .map { countCoOccurences }
      .reduceByKey { _ + _ }
      .map { x => (x._2, x._1) }
      .max()

    val mostPopularName = namesRdd.lookup(mostPopular._2).head

    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.") 
  }
  
}
