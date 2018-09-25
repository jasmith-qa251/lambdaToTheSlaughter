package com.example

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.functions.{avg, col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchProcessing {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("batch process")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val SERVER = "localhost:6001"
  val TOPIC_NAME = "test_topic"

  /**
    * Adds flag when the row's target column is twice as large as the average for the whole.
    *
    * @param table String - Spark table name to read from.
    * @param column String - Name of column to search.
    * @return DataFrame
    */
  def findOutliers(table: String, column: String): DataFrame = {

    spark
      .sql("SELECT * FROM memory")
      .withColumn("outlierFlag", when(col("target").geq(avg("target") * 2), "outlier").otherwise("-"))
  }

  def main(args: Array[String]): Unit = {

    EmbeddedKafka.start()

    // Read Kafka topic and write to Kafka topic.
    spark
      .read
      .json("data.json")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("topic", TOPIC_NAME)
      .save()

    // Read Kafka topic and write to memory table.
    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("subscribe", TOPIC_NAME)
      .load()
      .selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")
      .writeStream
      .format("memory")
      .queryName("memory_raw")
      .start()

    // Start timers.
    var t0_process = System.nanoTime()
    var t0_persist = System.nanoTime()

    while (kafkaDF.isActive) {

      // Start timer in loop.
      val t1_process = System.nanoTime()
      val t1_persist = System.nanoTime()
      val outlierDF = findOutliers("memory_raw", "target")

      // If 10 seconds elapsed...
      if (t1_process - t0_process > 10000000000L) {

        outlierDF.createOrReplaceGlobalTempView("memory_processed")
        t0_process = t1_process // Restart timer from current time.
      }

      // If 100 seconds elapsed...
      if (t1_persist - t0_persist > 100000000000L) {

        outlierDF.createOrReplaceGlobalTempView("hive_temporary")
        t0_persist = t1_persist // Restart timer from current time.
      }

      // Delay next loop by 5 seconds.
      Thread.sleep(5000)
    }

    EmbeddedKafka.stop()

    // Perform final refresh when stream stops.
    val processedDF = findOutliers("memory", "target")
    processedDF.createOrReplaceGlobalTempView("memory_processed")
    processedDF.createOrReplaceGlobalTempView("hive_temporary")

    spark.sql("SELECT * FROM memory_raw").show()
    spark.sql("SELECT * FROM memory_processed").show()
    spark.sql("SELECT * FROM hive_temporary").show()
  }
}
