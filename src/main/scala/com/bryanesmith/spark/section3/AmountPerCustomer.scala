package com.bryanesmith.spark.section3

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AmountPerCustomer {

  // Format: customer id, product id, amount
  private def parse(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", getClass.getSimpleName)

    sc.resourceTextFile("/section3/customer-orders.csv")
      .map(parse)                 // [ (customer, amount) ]
      .reduceByKey { _ + _ }      // [ (customer, total amount) ]
      .map(e => (e._2, e._1))
      .sortByKey()
      .foreach { e =>
        println(f"${e._2}%05d => $$${e._1}%.2f")
      }
  }
}
