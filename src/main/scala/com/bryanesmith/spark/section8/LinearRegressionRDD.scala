package com.bryanesmith.spark.section8

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object LinearRegressionRDD {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "LinearRegression")

    // x,y number pairs where x is the label, and y is the feature.
    // Note that the data file contains values scaled to -1 to 1 with mean of 0.
    val allData = sc.resourceTextFile("/section8/regression.txt")
      .randomSplit(Array(0.8, 0.2))

    val trainingData = allData(0)
      .map(LabeledPoint.parse).cache

    val testData = allData(1).map(LabeledPoint.parse)

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(0.01)

    val pairs = algorithm.run(trainingData)
      .predict(testData.map { _.features })
      .zip(testData.map { _.label })
      .collect

    pairs foreach println

    val error = (1.0 / pairs.length) * pairs.foldLeft(0.0) { (sum, next) =>
      sum + Math.pow(next._1 - next._2, 2)
    }

    println
    println(s"mean squared error: $error (pairs: ${pairs.length})")
  }
}