package com.bryanesmith.spark.section3

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Processing weather station data from year 1800
  */
object AggregateTemperatures {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val rdd = new SparkContext("local[*]", "MinTemperatures")
      .resourceTextFile("/section3/1800.csv")

    findTemp(rdd, AggregateType.Max)
  }

  /**
    * Find's specified aggregate type within specified RDD.
    */
  private def findTemp(rdd: RDD[String], aggregateType: AggregateType.Value): Unit =
    rdd.map(Entry.apply)
      .filter(_.entryType == aggregateType.entryType)
      .map(x => (x.stationId, x.tempF))
      .reduceByKey(aggregateType.fn)
      .collect
      .sorted
      .foreach { r =>
        val temp = f"${r._2}%.2f F"
        println(s"${r._1} minimum temperature: $temp")
      }

  /**
    * Temperature aggregation types.
    */
  private object AggregateType extends Enumeration {

    val Min, Max = Value

    implicit class AggregateTypeOps(value: Value) {

      def entryType: String = value match {
        case Min => "TMIN"
        case Max => "TMAX"
      }

      def fn: (Float, Float) => Float = value match {
        case Min => Math.min
        case Max => Math.max
      }

    } // AggregateTypeOps

  } // AggregateType

  /**
    * Represents an entry in the provided data.
    */
  private case class Entry (
    stationId: String,
    date: String,
    entryType: String,
    tempF: Float
  )

  private object Entry {
    def apply(line: String): Entry = {
      val fields = line.split(",")
      Entry(
        stationId = fields(0),
        date = fields(1),
        entryType = fields(2),
        tempF = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
      )
    }
  }

} // AggregateTemperatures