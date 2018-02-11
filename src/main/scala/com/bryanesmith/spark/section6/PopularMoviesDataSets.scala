package com.bryanesmith.spark.section6

import java.nio.charset.CodingErrorAction

import com.bryanesmith.spark.IOUtils
import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesDataSets {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val path = IOUtils.resourceFilePath("/section1/ml-100k/u.item")

    Source.fromFile(path).getLines
      .map { _.split('|') }
      .filter { _.length > 1 }
      .map { fields => (fields(0).toInt, fields(1)) }
      .toMap
  }

  final case class Movie(movieID: Int)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext
      .resourceTextFile("/section1/ml-100k/u.data")
      .map { line =>
        val tokens = line.split("\t")
        Movie(tokens(1).toInt)
      }

    import spark.implicits._
    val moviesDS = lines.toDS()

    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()

    // Show the results at this point:
    /*
    |movieID|count|
    +-------+-----+
    |     50|  584|
    |    258|  509|
    |    100|  508|
    */

    topMovieIDs.show()

    // Grab the top 10
    val top10 = topMovieIDs.take(10)

    // Load up the movie ID -> name map
    val names = loadMovieNames()

    // Print the results
    println
    for (result <- top10) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    // Stop the session
    spark.stop()
  }

}