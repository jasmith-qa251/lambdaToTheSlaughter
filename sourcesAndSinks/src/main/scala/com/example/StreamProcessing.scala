package com.example


import java.io.File

import org.apache.spark.sql.functions.{avg, col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._
import scala.reflect.io.Directory



object StreamProcessing {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("parkProducerDemoAverage").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

  //configuration for kafka and hive(Note:this will be replaced by kudu) start
  val SERVER = "localhost:9092"
  val TOPIC_IN = "streaming-demo-averageweeklyhouseholdspend"
  val TOPIC_OUT = "testOut_topic"
  val HIVE_DB = "default"
  val HIVE_TABLE = "household_weekly_spend"
  val VALUE_COL_NAME = "averageweeklyhouseholdspend"
  val FLAG_COL_NAME = "is_above_average"
  val YES = "yes"
  val NO = "no"
  //configuration for kafka and hive(Note:this will be replaced by kudu) end

  val SCHEMA_IN = StructType(Seq(
    StructField("userid", StringType, nullable = true),
    StructField("username", StringType, nullable = true),
    StructField("averageweeklyhouseholdspend", IntegerType, nullable = true)
  ))
  val SCHEMA_OUT = StructType(Seq(
    StructField("userid", StringType, nullable = true),
    StructField("username", StringType, nullable = true),
    StructField("averageweeklyhouseholdspend", IntegerType, nullable = true),
    StructField(FLAG_COL_NAME, StringType, nullable = true)
  ))

  /**
    * This method is to read the threshold household_spending_average from hive table.
    *
    * @param databaseName -String - Hive database name
    * @param tableName    - String - Hive table name
    * @param ColName      - String - name od the column from which average will be calculated
    * @return
    */
  def readHive(databaseName: String, tableName: String, ColName: String): DataFrame = {

    spark.sql("SELECT * FROM " + databaseName + "." + tableName).select(avg(col(ColName)))

  }


  /**
    * This method is to add a flag column with value "Y" or "N".
    * If the value is greater than threshold average, set flag ='Y'.otherwise set flag
    *
    * @param inDf           - Dataframe - Input
    * @param thresholdValue - Double - Threshold average
    * @param colName        - String - Name of the column which will be compared with threshold value
    * @return
    */
  def streamProcessMethod(inDf: DataFrame, thresholdValue: Double, colName: String): DataFrame = {
    inDf.withColumn(FLAG_COL_NAME,
      when(col(colName).isNotNull and col(colName) >= thresholdValue, YES).otherwise(NO))
  }

  /**
    * Entry method to Stream processing
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //Todo Delete
    println("Before clean-up checkpoint directory")
    val directory = new Directory(new File("tmp/checkpoint/"))
    directory.deleteRecursively()
    println("After clean-up checkpoint directory")


    // create a hive table by reading the data from Json start
    val dfHive = spark.read.json("HiveTable.json")
    dfHive.write.mode("Overwrite") saveAsTable (HIVE_DB + "." + HIVE_TABLE)
    //Hive table write end
    //Todo delete

    //Read kafka topic_in and create stream dataframe
    val inStreamNewDf = spark
      .readStream
      //.read
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("subscribe", TOPIC_IN)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss",false)
      .load()

    //Apply schema for input streamDataframe
    val dfNewTemp = inStreamNewDf.selectExpr("CAST(value as STRING) value")
      .as[String]
      .select(functions.from_json(col("value"), SCHEMA_IN).as("data")).select("data.*")
    println(" Read data from topic in ....")

    //Read threshold average from hive table
    val avg = readHive(HIVE_DB, HIVE_TABLE, VALUE_COL_NAME).select("avg(" + VALUE_COL_NAME + ")").as[String].collect()
    println("Threshold average ::: " + avg(0))

    //Apply the processing method
    val dfProcessed = streamProcessMethod(dfNewTemp, avg(0).toDouble, VALUE_COL_NAME)

    val displayQuery = dfProcessed.writeStream.format("console").option("checkpointLocation", "tmp/checkpoint"). // <-- checkpoint directory
      trigger(Trigger.ProcessingTime(10.seconds)).
      outputMode(OutputMode.Update())
      .start()
    //Write to kafka topic_out
    val writeQuery = dfProcessed
      .selectExpr("CAST(userid as STRING) AS key", "to_json(struct(*)) AS value")
      //.write
      .writeStream
      .format("kafka")
      .option("topic", TOPIC_OUT)
      .option("kafka.bootstrap.servers", SERVER)
      .option("checkpointLocation", "checkpointDir")
      .option("auto.offset.reset", "earliest")
      .start()
    //displayQuery.awaitTermination()
    writeQuery.awaitTermination()
    displayQuery.awaitTermination()
    //Todo delete end
  }

}
