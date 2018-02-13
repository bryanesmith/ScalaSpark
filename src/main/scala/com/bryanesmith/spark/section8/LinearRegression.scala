package com.bryanesmith.spark.section8

import com.bryanesmith.spark.SparkContextExtras._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object LinearRegression {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "LinearRegression")

    // x,y number pairs where x is the label, and y is the feature.
    // we'll scale to -1 to 1 with mean of 0.

    val trainingData = sc.resourceTextFile("/section8/regression.txt")
      .map(LabeledPoint.parse).cache()

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(0.01)

    val model = algorithm.run(trainingData)

    // TODO: separate testing, training data
    val testData = sc.resourceTextFile("/section8/regression.txt")
      .map(LabeledPoint.parse)

    // Predict values for our test feature data using our linear regression model
    val predictions = model.predict(testData.map(_.features))
    
    // Zip in the "real" values so we can compare them
    val predictionAndLabel = predictions.zip(testData.map(_.label))
 
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
  }
}