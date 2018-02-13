package com.bryanesmith.spark.section8

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql._

object LinearRegressionDataFrame {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    val inputLines = spark.sparkContext.resourceTextFile("/section8/regression.txt")

    val data = inputLines.map{ _.split(",") }
      .map { x => (x(0).toDouble, Vectors.dense { x(1).toDouble }) }

    import spark.implicits._
    val trainTest = data.toDF("label", "features")
      .randomSplit(Array(0.8, 0.2))

    val pairs = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6)
      .fit(trainTest(0))
      .transform(trainTest(1)).cache()
      .select("prediction", "label").rdd
      .map { x => (x.getDouble(0), x.getDouble(1)) }
      .collect

    pairs foreach println

    val error = (1.0 / pairs.length) * pairs.foldLeft(0.0) { (sum, next) =>
      sum + Math.pow(next._1 - next._2, 2)
    }

    println
    println(s"mean squared error: $error (pairs: ${pairs.length})")

    spark.stop()
  }
}