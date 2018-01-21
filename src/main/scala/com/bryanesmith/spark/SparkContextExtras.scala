package com.bryanesmith.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkContextExtras {

  implicit class SparkContextOps(sc: SparkContext) {

    /**
      * Helper method to call `textFile` on a resource file.
      */
    def resourceTextFile(resourcePath : String) : RDD[String] =
      sc.textFile(IOUtils.resourceFilePath(resourcePath))
  }
}
