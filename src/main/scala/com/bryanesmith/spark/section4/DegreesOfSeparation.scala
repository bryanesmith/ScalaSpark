package com.bryanesmith.spark.section4

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator

import scala.annotation.tailrec

/**
  * Finds the degrees of separation between two Marvel comic book characters, based
 *  on co-appearances in a comic.
 */
object DegreesOfSeparation {

  val startCharacterID  = 5306  // SpiderMan
  val targetCharacterID = 14    // ADAM 3,031 (who?)

  type BFSNode = (Int, BFSData)
  case class BFSData(
    edges: Array[Int],
    distance: Int,
    color: String
  )
    
  /**
    * Converts a line of raw input into a BFSNode
    */
  def convertToBFS(line: String): BFSNode = {

    val fields = line.split("\\s+")

    val heroID = fields.head.toInt
    val connections = fields.tail.map { _.toInt }

    if (heroID == startCharacterID) {
      (heroID, BFSData(connections, 0, "GRAY"))
    } else {
      (heroID, BFSData(connections, 9999, "WHITE"))
    }
  }
  
  /**
    * Expands a BFSNode into this node and its children
    */
  def bfsMap(node:BFSNode)(implicit hitCounter: LongAccumulator): Array[BFSNode] =
    node._2.color match {
      case "GRAY" =>
        node._2.edges.map { // All connections become gray, and current node becomes black
          (_, BFSData(Array[Int](), node._2.distance + 1, "GRAY"))
        } :+ (node._1, node._2.copy(color = "BLACK"))
      case _ => Array(node)
    }
  
  /**
    * Reducer combines data for each character ID, preserving the darkest color and
    *   shortest path.
    */
  def bfsReduce(data1:BFSData, data2:BFSData): BFSData =
    BFSData(
      edges = data1.edges ++ data2.edges,
      distance = Math.min(data1.distance, data2.distance),
      color = Array("BLACK", "GRAY").find { c => c == data1.color || c == data2.color }
        .getOrElse("WHITE")
    )

  @tailrec
  def iteration(
    count: Int,
    max: Int,
    iterationRdd: RDD[BFSNode]
  )(implicit hitCounter: LongAccumulator): Unit = {

    println("Running BFS Iteration# " + count)

    val mapped = iterationRdd.flatMap { bfsMap(_) }

    // Did we find the target character?
    mapped.filter { _._1 == targetCharacterID }
        .filter { _._2.color == "GRAY" }
        .foreach { _ =>
          hitCounter.add(1)
        }

    println("Processing " + mapped.count() + " values.")

    val hitCount = hitCounter.value
    if (hitCount > 0) {
      println("Hit the target character! From " + hitCount +
        " different direction(s).")
    } else if (hitCount < max) {
      iteration(count + 1, max, mapped.reduceByKey(bfsReduce))
    }
  } // iteration

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "DegreesOfSeparation") 
    
    // Used to signal when we find the target character
    implicit val hitCounter: LongAccumulator = sc.longAccumulator("Hit Counter")

    val inputFile = sc.resourceTextFile("/section4/marvel-graph.txt")

    iteration(1, 10, inputFile.map(convertToBFS))

  } // main

} // DegreesOfSeparation