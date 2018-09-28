package com.example

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Container {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lambda demo")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  val KAFKA_SERVER = "localhost:6001" // TODO: Update with port.

  val KUDU_MASTER = "localhost:0000" // TODO: Update with port.
  val kuduContext: KuduContext = new KuduContext(KUDU_MASTER, spark.sparkContext)

  object Batch {

    /**
      * Read from Spark view, apply method via reflection, write to Kudu table.
      *
      * @param sourceView String - Name of Spark temporary view source
      * @param sinkTable String - Name of Kudu table
      */
    def sparkViewToKudu(sourceView: String, sinkTable: String): Unit = {

      // Call read method.
      val df = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "readFromSparkView",
        inParam = Map("view" -> sourceView)
      )

      // Call transformation method.
      val dfMethod = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "findOutliers",
        inParam = Map("df" -> df, "column" -> BatchProcessing.COLUMN_TO_VALIDATE)
      )

      // Call write method.
      Reflections.getObjectMethodNew[Unit](
        objectName = "com.example.BatchProcessing",
        methodName = "writeToKuduTable",
        inParam = Map("df" -> dfMethod, "table" -> sinkTable)
      )
    }

    /**
      * Read from source, apply method via reflection, write to sink.
      *
      * @param sourceView String - Name of Spark temporary view source
      * @param sinkView String - Name of Spark temporary view sink
      */
    def sparkViewToView(sourceView: String, sinkView: String): Unit = {

      // Call read method.
      val df = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "readFromSparkView",
        inParam = Map("view" -> sourceView)
      )

      // Call transformation method.
      val dfMethod = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "findOutliers",
        inParam = Map("df" -> df, "column" -> BatchProcessing.COLUMN_TO_VALIDATE)
      )

      // Call write method.
      Reflections.getObjectMethodNew[Unit](
        objectName = "com.example.BatchProcessing",
        methodName = "writeToSparkView",
        inParam = Map("df" -> dfMethod, "view" -> sinkView)
      )
    }
  }

  object Stream {

    /**
      * Read from source, apply method via reflection, write to sink.
      *
      * @param sourceTopic String - Name of Kafka topic source
      * @param sinkView String - Name of Spark temporary view sink
      */
    def kafkaToSparkView(sourceTopic: String, sinkView: String): Unit = {

      // Call read method.
      val df = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.KafkaIntegration",
        methodName = "readFromTopic",
        inParam = Map("server" -> KAFKA_SERVER, "topic" -> BatchProcessing.KAFKA_TOPIC)
      )

      // Call transformation method.
      val dfMethod = Reflections.getObjectMethodNew[DataFrame](
        objectName = "com.example.BatchProcessing",
        methodName = "selectFromJSON",
        inParam = Map("df" -> df)
      )

      // Call write method.
      Reflections.getObjectMethodNew[Unit](
        objectName = "com.example.BatchProcessing",
        methodName = "writeStreamToSparkView",
        inParam = Map("df" -> dfMethod, "view" -> sinkView)
      )
    }
  }
}
