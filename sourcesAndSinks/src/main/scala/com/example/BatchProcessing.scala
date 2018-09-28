package com.example


import java.time.Instant

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BatchProcessing {

  var KAFKA_TOPIC: String = _
  var COLUMN_TO_VALIDATE: String = _
  var PROCESS_INTERVAL: Long = _
  var PERSIST_INTERVAL: Long = _

  val SCHEMA = StructType(Seq(
    StructField("userid", StringType, nullable = true),
    StructField("username", StringType, nullable = true),
    StructField("averageweeklyhouseholdspend", IntegerType, nullable = true)
  ))

  Container.spark.sparkContext.setLogLevel("ERROR")
  import Container.spark.implicits._

  /**
    * Sets CLI arguments to static members.
    *
    * @param args Array[String] - CLI arguments
    */
  def setCLIArgs(args: Array[String]): Unit = {

    try {

      KAFKA_TOPIC = args(0).trim
      COLUMN_TO_VALIDATE = args(1).trim
      PROCESS_INTERVAL = args(2).trim.toLong
      PERSIST_INTERVAL = args(3).trim.toLong
    }
    catch {

      case _: Throwable =>
        println("""Usage: [kafkaTopic String] [columnToValidate String] [processInterval Long] [persistInterval Long]""")
        System.exit(1)
    }
  }

  /**
    * Read from Spark temporary view.
    *
    * @param view - Name of Spark view
    * @return DataFrame
    */
  def readFromSparkView(view: String): DataFrame = Container.spark.sql("SELECT * FROM " + view)

  /**
    * Adds flag with comparison of target column with average for whole set.
    *
    * @param df DataFrame - Data to process
    * @param column String - Name of column to search
    * @return DataFrame
    */
  def findOutliers(df: DataFrame, column: String): DataFrame = {

    if (df.head(1).isEmpty) df
    else df
      .withColumn("average", lit(df.agg(avg(column)).head.getDouble(0)))
      .withColumn("is_above_average", when(col(column) >= col("average"), "yes").otherwise("no"))
      .drop("average")
  }

  /**
    * Transformation method used when reading JSON from Kafka.
    *
    * @param df DataFrame - Data containing 'value' column in JSON form.
    * @return DataFrame
    */
  def selectFromJSON(df: DataFrame): DataFrame = {

    df
      .selectExpr("CAST(value as STRING) value")
      .as[String]
      .select(from_json(col("value"), SCHEMA).as("data")).select("data.*")
  }

  /**
    * Write to Spark temporary view.
    *
    * @param df DataFrame - Data to write
    * @param view String - Name of Spark view
    */
  def writeToSparkView(df: DataFrame, view: String): Unit = {

    df.createOrReplaceTempView(view)
    println(view.split("_")(1).capitalize + " data:")
    df.show()
  }

  /**
    * Write a streaming DataFrame to memory.
    *
    * @param df DataFrame - streaming Data to write.
    * @param view String - Name of Spark view.
    */
  def writeStreamToSparkView(df: DataFrame, view: String): Unit = {

    df
      .writeStream
      .format("memory")
      .queryName(view)
      .start()
  }

  /**
    * Write to Kudu table.
    *
    * @param df DataFrame - Data to write
    * @param table String - Name of Kudu table
    */
  def writeToKuduTable(df: DataFrame, table: String): Unit = {

    Container.kuduContext.upsertRows(df, table)
    println("Persisted data:")
    df.show()
  }

  def main(args: Array[String]): Unit = {

    setCLIArgs(args)

    Container.Stream.kafkaToSparkView(KAFKA_TOPIC, "memory_raw") // Read Kafka topic and write to memory table.

    var t0_process, t0_persist = System.currentTimeMillis() // Store initial time for determining intervals.

    // TODO: Set logic to stop if needed.
    // If enough time has elapsed each loop, perform the batch processes.
    while (true) {

      val t1_process, t1_persist = System.currentTimeMillis() // Update timers each loop.

      // Read latest raw data.
      println(s"<< BATCH KICKOFF @ ${Instant.ofEpochMilli(t1_process).toString.dropRight(5).replace("T", " ") + "[UTC]"} >>\n")
      println("Raw data:")
      Container.spark.sql("SELECT * FROM memory_raw").show()

      // When time elapsed reaches PROCESS_INTERVAL...
      if (t1_process - t0_process >= PROCESS_INTERVAL) {

        Container.Batch.sparkViewToView("memory_raw", "memory_processed")
        t0_process = t1_process
      }

      // When time elapsed reaches PERSIST_INTERVAL...
      if (t1_persist - t0_persist >= PERSIST_INTERVAL) {

        Container.Batch.sparkViewToKudu("memory_raw", "kudu_persisted") // Requires that this table exists.
        t0_persist = t1_persist
      }

      Thread.sleep(PROCESS_INTERVAL) // Delay next loop.
    }
  }
}
