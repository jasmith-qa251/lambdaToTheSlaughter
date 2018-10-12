package com.example

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{SparkSession, functions}



object ServiceLayer {

  def main(args: Array[String]): Unit = {

    //Kafka Setup
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "my-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val kafkaConsumer = new KafkaConsumer[String, String](properties)


    val spark = SparkSession.builder().config("spark.local.dir", "C:/tmp").master("local[1]").getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("averageweeklyhouseholdspend", IntegerType, true),
      StructField("is_above_average", StringType, true),
      StructField("userid", StringType, true),
      StructField("username", StringType, true)
    ))

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testOut_topic")
      .load()
    df.printSchema()


    val dfkafka = df.selectExpr("CAST(value as STRING)")
      .as[String]
      .select(functions.from_json($"value", schema).as("data")).select("data.*")




    //Kudu Setup
    val kuduContext = new KuduContext("localhost:7051", spark.sparkContext)


    val dfkudu = spark.read.options(Map("kudu.master" -> "localhost:7051", "kudu.table" -> "test_table")).kudu

    println("Kafka Data")
    dfkafka.show()//withColumn("Source", lit("kafka")).show()

    println("Kudu Data")
    dfkudu.show()//withColumn("Source", lit("kudu")).show()

    val stream_output = dfkafka.join(dfkudu, dfkafka.col("userid")=== dfkudu.col("userid"), "leftanti")

    println("Stream Output Schema")
    stream_output.printSchema()

    println("Stream Output Data")
    stream_output.show()

    val newdf = stream_output.select("userid","username", "averageweeklyhouseholdspend", "is_above_average" ).withColumn("Source", lit("kafka"))
    val newdf2 = dfkudu.select("userid","username", "averageweeklyhouseholdspend", "is_above_average").withColumn("Source", lit("kudu"))

    val df_final = newdf.union(newdf2)
    println("final data")
    df_final.show()


  }
}