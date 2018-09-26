package com.example

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchProcessing {

  var SERVER: String = _
  var KAFKA_TOPIC: String = _
  var COLUMN_TO_VALIDATE: String = _
  var PROCESS_INTERVAL: Long = _
  var PERSIST_INTERVAL: Long = _

  val SCHEMA = StructType(Seq(
    StructField("time", StringType),
    StructField("reporting_unit", StringType),
    StructField("turnover", FloatType)
  ))

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("batch process")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  /**
    * Sets CLI arguments to static members.
    *
    * @param args Array[String] - CLI arguments
    */
  def setCLIArgs(args: Array[String]): Unit = {

    try {

      SERVER = args(0).trim
      KAFKA_TOPIC = args(1).trim
      COLUMN_TO_VALIDATE = args(2).trim
      PROCESS_INTERVAL = args(3).toLong
      PERSIST_INTERVAL = args(4).toLong
    }
    catch {

      case _: Throwable =>
        println("""Usage: [server String] [kafkaTopic String] [columnToValidate String] [processInterval Long] [persistInterval Long]""")
        System.exit(1)
    }
  }

  /**
    * Adds flag when the row's target column is twice as large as the average for the whole.
    *
    * @param table String - Spark table name to read from
    * @param column String - Name of column to search
    * @return DataFrame
    */
  def findOutliers(table: String, column: String): DataFrame = {

    val df = spark.sql("SELECT * FROM " + table)

    if (df.head(1).isEmpty) df
    else df
      .withColumn("average", lit(df.agg(avg(column)).head.getDouble(0)))
      .withColumn("outlier_flag", when(col(column) >= col("average") * 2, "outlier").otherwise("-"))
      .drop("average")
  }

  def main(args: Array[String]): Unit = {

    setCLIArgs(args)

    // TODO: REMOVE
    EmbeddedKafka.start()

    // TODO: REMOVE
    // Read JSON file and write to Kafka topic.
    spark
      .read
      .json("data.json")
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("topic", KAFKA_TOPIC)
      .save()

    // Read Kafka topic and write to memory table.
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value as STRING) value")
      .as[String]
      .select(from_json(col("value"), SCHEMA).as("data")).select("data.*")
      .writeStream
      .format("memory")
      .queryName("memory_raw")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    // Store initial time for determining intervals.
    var t0_process = System.currentTimeMillis()
    var t0_persist = System.currentTimeMillis()
    val continue = true // TODO: Set logic to stop if needed.

    while (continue) {

      // Update timer each loop.
      val t1_process = System.currentTimeMillis()
      val t1_persist = System.currentTimeMillis()

      // Read latest raw data.
      println("Raw data at system time " + t1_process + ":")
      spark.sql("SELECT * FROM memory_raw").show()

      // Process raw data.
      val outlierDF = findOutliers("memory_raw", COLUMN_TO_VALIDATE)

      // If 10 seconds elapsed...
      if (t1_process - t0_process >= PROCESS_INTERVAL) {

        // Replace view with DF, show new data.
        outlierDF.createOrReplaceTempView("memory_processed")
        println("Processed data at system time " + t1_process + ":")
        spark.sql("SELECT * FROM memory_processed").show()

        // Restart timer from current time.
        t0_process = t1_process
      }

      // If 50 seconds elapsed...
      if (t1_persist - t0_persist >= PERSIST_INTERVAL) {

        // Replace view with DF, show new data.
        outlierDF.createOrReplaceTempView("hive_temporary")
        println("Persisted data at system time " + t1_persist + ":")
        spark.sql("SELECT * FROM hive_temporary").show()

        // Restart timer from current time.
        t0_persist = t1_persist
      }

      // Delay next loop by 5 seconds.
      Thread.sleep(PROCESS_INTERVAL)
      println("<<< NEW BATCH KICKOFF >>>")
    }
  }
}