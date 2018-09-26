package Producer;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Duration;
import java.time.LocalTime;

import java.util.*;

public class EventStreaming {

    public static void main(String[] args) {
        final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

        System.out.println("\n" + "------------------------------------" +"\n");
        System.out.println("\n" + "EVENT STREAMING DEMO INITIALISING..." +"\n");
        System.out.println("\n" + "------------------------------------" +"\n");

        //Setup Spark:
        String master = "local[1]";
        String appName = "SparkProducerDemoAverage";
        final SparkSession spark = SparkSession.builder().appName(appName).master(master).getOrCreate();
        System.out.println("Spark Version In Play: "+spark.version());

        //Setup Schema:
        StructField[] structFields = new StructField[]{
                new StructField("userid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("username", DataTypes.StringType, true, Metadata.empty()),
                new StructField("averageweeklyhouseholdspend", DataTypes.IntegerType, true, Metadata.empty())
        };
        final StructType structType = new StructType(structFields);

        //Setup Kafka:
        final String strKafkaLocalHost = "localhost:9092";
        final String strKafkaTopic = "streaming-demo-averageweeklyhouseholdspend";

        final String groupId = "my-group", topic = strKafkaTopic, type = "";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, strKafkaLocalHost);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        final int intTimerInitialDelay = 0; //Seconds...
        final int intTimerDelay = 10; //Seconds...
        final int intMaxRecordsPerBatch = 10; //Maximum per Batch...


        System.out.println("\n"+"Event Streaming Spinning Up..."+"\n");
        final LocalTime startTime = LocalTime.now();

        ses.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {

                Date currentTime = new Date();
                System.out.println("New Cycle Production Started on: " + new Date());
                Duration duration = Duration.between(startTime, LocalTime.now());
                if (duration.toMinutes() < 1) {
                    System.out.println("Approximate Running Time: Less than one minute.");
                } else if (duration.toMinutes() == 1) {
                    System.out.println("Approximate Running Time: About one minute.");
                }
                else if (duration.toMinutes() > 1 && duration.toHours() < 1) {
                    System.out.println("Approximate Running Time: " + duration.toMinutes() + " minutes.");
                }
                else {
                    System.out.println("Approximate Running Time: " + duration.toHours() + " hour(s).");
                    System.out.println("Wow, this is one in-depth demonstration session...");
                }
                final Dataset<Row> df = spark.createDataFrame(RecordResources.getInitialiseData(intMaxRecordsPerBatch,2),structType); // RecordResources.getInitialiseData(intMaxRecordsPerBatch, 2),structType);
                System.out.println("Generated Record(s) Sent to Kafka Topic: " + topic + "...");
                df
                        .selectExpr("CAST(userid AS INTEGER)", "to_json(struct(*)) AS value")
                        .write() //writestream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", strKafkaLocalHost)
                        .option("topic", topic)
                        .save(); //start
                df.show(intMaxRecordsPerBatch);


            }
        }, intTimerInitialDelay, intTimerDelay, TimeUnit.SECONDS);


    }

}
