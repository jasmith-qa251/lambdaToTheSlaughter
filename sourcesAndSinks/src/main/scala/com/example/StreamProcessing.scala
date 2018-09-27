package com.example

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.functions.{avg, col, when}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{
  StructType, StructField, StringType, IntegerType}


object StreamProcessing {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("stream process")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val SERVER = "localhost:6001"
  val TOPIC_IN = "testIn_topic"
  val TOPIC_OUT = "testOut_topic"

  def readHive(databaseName:String,tableName:String,ColName:String): DataFrame ={

    //Todo delete write table start
    spark.sql("SELECT * FROM "+databaseName + "." + tableName).select(avg(col(ColName)))
    //Todo delete write table end

  }

  def streamProcessMethod(inDf:DataFrame,thresholdValue:Double,colName:String):DataFrame ={
    inDf.show()
    inDf.withColumn("flag_above_avg_salary",
        when(col(colName).isNotNull and col(colName) >= thresholdValue,"Y").otherwise("N"))
  }

  def main(args: Array[String]): Unit = {

    EmbeddedKafka.start()
    import spark.implicits._
    //spark.sql("SELECT * FROM default.test_avg_table")
    //Todo Delete write logic to delete start
    spark
      .read
      .json("data_stream.json").selectExpr("CAST(key as STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("topic", TOPIC_IN)
      .save()
    val dfHive = spark.read.json("HiveTable.json")
    dfHive.write.mode("Overwrite") saveAsTable("default" +"."+ "test_avg_table")
    //Todo delete write logic end

    // Read Kafka topic and write to Kafka topic.
    val inStreamDf = spark
      //.readStream
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("subscribe", TOPIC_IN)
      .option("startingOffsets","earliest")
      .load().selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")
    val schema = StructType(Array(
      StructField("key",StringType,false),
      StructField("value",IntegerType,true)
    )
    )
    val dfTemp = inStreamDf.selectExpr("CAST(value as STRING)")
      .as[String]
      .select(functions.from_json($"value", schema).as("data")).select("data.*")
    val avg = readHive("default","test_avg_table","value").select("avg(value)").as[String].collect()
    //println("hiii " + avg(0))
    val dfProcessed = streamProcessMethod(dfTemp,avg(0).toDouble,"value")
    dfProcessed.show()
    //inStreamDf.writeStream.format("console").start().awaitTermination()
     dfProcessed
      .selectExpr("CAST(key as STRING) AS key", "to_json(struct(*)) AS value" )
      .write
      .format("kafka")
      .option("topic", TOPIC_OUT)
      .option("kafka.bootstrap.servers", SERVER)
      .option("auto.offset.reset", "earliest")
      .save()
    val schemaOut = StructType(Array(
      StructField("key",StringType,false),
      StructField("value",IntegerType,true),
      StructField("flag_above_avg_salary",StringType)
    )
    )
       val testOutDf = spark
      //.readStream
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", SERVER)
      .option("subscribe", TOPIC_OUT)
      .option("startingOffsets","earliest")
      .load()
      .selectExpr("CAST(key as STRING) AS key","CAST(value AS STRING) value")
       testOutDf.show()


    EmbeddedKafka.stop()

    // Perform final refresh when stream stops.

  }
}
