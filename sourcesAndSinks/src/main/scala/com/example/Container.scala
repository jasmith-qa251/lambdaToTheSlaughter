package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Container {

  object BatchSparkViewToView {

    /**
      * Read from source, apply method via reflection, write to sink.
      *
      * @param sourceView String - Name of Spark temporary view source.
      * @param sinkView String - Name of Spark temporary view sink.
      */
    def execute(sourceView: String, sinkView: String): Unit = {

      // Get parameters from orchestration object.
      val session: SparkSession = BatchProcessing.spark
      val column: String = BatchProcessing.COLUMN_TO_VALIDATE

      val df = session.sql("SELECT * FROM " + sourceView)

      // Call reflections method.
      val dfMethod = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "findOutliers",
        inParam = Map("df" -> df, "column" -> column))

      // Write output to sink.
      dfMethod.createOrReplaceTempView(sinkView)
      println(getSuffix(sinkView).capitalize + " data:")
      dfMethod.show()
    }

    /**
      * Assumes an input string of 'abc_xyz,' returns 'xyz.'
      *
      * @param string String - Phrase containing '_' separator
      * @return String
      */
    def getSuffix(string: String): String = string.split("_")(1)
  }
}
