package com.example

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object KafkaIntegration{

  def readFromTopic(server: String, topic: String) : DataFrame = {

    Container.spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  def writeToTopic(streamDF:DataFrame, topic:String, checkpointDir:String): StreamingQuery = {

    val ds = streamDF.selectExpr("CAST(id as STRING) AS key", "to_json(struct(*)) AS value" )
      .writeStream
      .format("kafka")
      .option("topic", topic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("auto.offset.reset", "earliest")
      .option("checkpointLocation", checkpointDir)
      .start()

    ds
  }

  def writeDFToTopic(dataFrame: DataFrame, topic:String, checkpointDir:String) = {

    dataFrame.selectExpr("CAST(id as STRING) AS key", "to_json(struct(*)) AS value" )
      .write
      .format("kafka")
      .option("topic", topic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("auto.offset.reset", "earliest")
      .option("checkpointLocation", checkpointDir)
      .save()
  }
}
