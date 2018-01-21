package com.bryanesmith.spark

object IOUtils {

  /**
    * Converts an absolute path within resources directory to filesystem path.
    * @param resourcePath E.g., "/section1/ml-100k/u.data"
    */
  def resourceFilePath(resourcePath : String) : String =
    this.getClass.getResource(resourcePath).getFile

}
