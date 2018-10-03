package Producer;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class JoinStreamBatchExamplev2 {

    public static void main(String[] args) {

        System.out.println("\n" + "------------------------------------" +"\n");
        System.out.println("\n" + "BATCH & STREAMING JOIN DEMO..." +"\n");
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
                new StructField("averageweeklyhouseholdspend", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("averagemarker", DataTypes.StringType, true, Metadata.empty())
        };
        final StructType structType = new StructType(structFields);

        List<Row> rowsbatch = new ArrayList<>();
        List<Row> rowsstream = new ArrayList<>();

        rowsbatch.add(RowFactory.create("Wed180926-18:50:26:723_0", "Jones_Poppy", 265, "-")); //599.8
        rowsbatch.add(RowFactory.create("Wed180926-18:50:26:723_1", "Wilkinson_Lucy", 772, "Above Average"));
        rowsbatch.add(RowFactory.create("Wed180926-18:50:26:723_2", "Wood_Hannah", 987, "Above Average"));
        rowsbatch.add(RowFactory.create("Wed180926-18:50:27:552_1", "Allen_George", 433, "-"));
        rowsbatch.add(RowFactory.create("Wed180926-18:50:27:552_2", "Smith_Jack", 542, "-"));

        rowsstream.add(RowFactory.create("Wed180926-18:50:27:552_1", "Allen_George", 433, "Above Average")); //344.2
        rowsstream.add(RowFactory.create("Wed180926-18:50:27:552_2", "Smith_Jack", 542, "Above Average"));
        rowsstream.add(RowFactory.create("Wed180926-18:57:45:223_0", "White_Grace", 113, "-"));
        rowsstream.add(RowFactory.create("Wed180926-18:57:45:223_1", "Evans_Emma", 200, "-"));
        rowsstream.add(RowFactory.create("Wed180926-18:57:45:223_2", "Williams_Anthony", 433, "Above Average")); //Would be above average when in Batch...


        final Dataset<Row> df_batch = spark.createDataFrame(rowsbatch, structType);
        final Dataset<Row> df_stream = spark.createDataFrame(rowsstream, structType);

        System.out.println("Batch Output:");
        df_batch.printSchema();
        df_batch.show(10);
        System.out.println("Stream Output:");
        df_stream.printSchema();
        df_stream.show(10);

        final Dataset<Row> df_stream_output = df_stream.join(df_batch, df_stream.col("userid").equalTo(df_batch.col("userid")), "leftanti").withColumn("source", functions.lit("Stream"));

        System.out.println("Get all Streamed Data that is not already processed in Batch (so only keep processed 'accurate data'):");
        df_stream_output.printSchema();
        df_stream_output.show(10);

        final Dataset<Row> df_batch2 = df_batch.withColumn("source", functions.lit("Batch"));
        final Dataset<Row> df_final_output = df_stream_output.union(df_batch2);
        System.out.println("Final View Across Batch and Stream:");
        df_final_output.printSchema();
        df_final_output.show(10);
        System.out.println("This final view should contain all records.");
        System.out.println("However George and Jack's data should only contain their data from the processed batch.");
        System.out.println("Therefore their data should show their weekly return is below average, not above average as in the streamed data.");
        System.out.println("Anthony's data has not been processed, so his average is an early indicator and not accurate until batch processed.");


    }
}

