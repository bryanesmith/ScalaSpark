package com.bryanesmith.spark.section3

import org.apache.spark._
import org.apache.log4j._
import com.bryanesmith.spark.SparkContextExtras._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  // Format: id, name, age, number friends
  private def ageAndFriends(line: String) = {
      val fields = line.split(",")
      (fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    sc.resourceTextFile("/section3/fakefriends.csv")
      .map(ageAndFriends)                   // [ (age, friends) ]
      .mapValues { (_, 1) }                 // [ (age, (friends, 1)) ]
      .reduceByKey { (x,y) =>               // [ (age, (total friends, count)) ]
        (x._1 + y._1, x._2 + y._2)
      }
      .sortByKey()                          // Sort by age
      .mapValues { x =>                     // [ (age, average) ]
        f"${x._1.toDouble / x._2}%.1f"
      }
      .collect
      .foreach(println)
  }
    
}
  