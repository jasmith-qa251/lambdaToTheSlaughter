package com.example

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchProcessing {

  var SERVER: String = _
  var KAFKA_TOPIC: String = _
  var COLUMN_TO_VALIDATE: String = _
  var PROCESS_INTERVAL: Long = _
  var PERSIST_INTERVAL: Long = _

  val SCHEMA = StructType(Seq(
    StructField("userid", StringType, nullable = true),
    StructField("username", StringType, nullable =  true),
    StructField("averageweeklyhouseholdspend", IntegerType, nullable =  true)
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
      PROCESS_INTERVAL = args(3).trim.toLong
      PERSIST_INTERVAL = args(4).trim.toLong
    }
    catch {

      case _: Throwable =>
        println("""Usage: [server String] [kafkaTopic String] [columnToValidate String] [processInterval Long] [persistInterval Long]""")
        System.exit(1)
    }
  }

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
      .withColumn("outlier_flag", when(col(column) >= col("average"), "Above Average").otherwise("Below Average"))
      .drop("average")
  }

  def main(args: Array[String]): Unit = {

    setCLIArgs(args)

    // TODO: REMOVE
    // Read from JSON and write to Kafka topic.
    EmbeddedKafka.start()
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
      println(s"<< BATCH KICKOFF @ ${t1_process}ms >>\n")
      println("Raw data:")
      spark.sql("SELECT * FROM memory_raw").show()

      // When PROCESS_INTERVAL is met...
      if (t1_process - t0_process >= PROCESS_INTERVAL) {

        Container.BatchSparkViewToView.execute("memory_raw", "memory_processed") // Call container process.
        t0_process = t1_process // Restart timer from current time.
      }

      // When PERSIST_INTERVAL is met...
      if (t1_persist - t0_persist >= PERSIST_INTERVAL) {

        Container.BatchSparkViewToView.execute("memory_raw", "memory_persisted") // Call container process.
        t0_persist = t1_persist // Restart timer from current time.
      }

      Thread.sleep(PROCESS_INTERVAL) // Delay next loop.
    }
  }
}