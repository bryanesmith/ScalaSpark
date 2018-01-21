package com.bryanesmith.spark.section4

import java.nio.charset.CodingErrorAction

import com.bryanesmith.spark.IOUtils
import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/**
  * Find the movies with the most ratings.
  */
object PopularMoviesNicer {

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

  def parseMovieId(line: String): Int = line.split("\t")(1).toInt

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    // Isn't necessary to use broadcast variable here; just a demo
    val nameDict = sc.broadcast { loadMovieNames() }

    sc.resourceTextFile("/section1/ml-100k/u.data")
      .map { line => (parseMovieId(line), 1) }    // [ (movie ID, 1) ]
      .reduceByKey { _ + _ }                      // [ (movie ID, count) ]
      .sortBy { x => x._2 }
      .map { x  => (nameDict.value(x._1), x._2) } // [ (movie name, count) ]
      .collect
      .foreach { println }
  }

} // PopularMoviesNicer