package com.morris.sparked.usbirthrates;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class USBirthRatesByCounty {
    private SparkSession sparkSession;

    /**
     * Application driver
     * @param args
     */
    public static void main(String[] args) {
        USBirthRatesByCounty app = new USBirthRatesByCounty();
        String mode = "noop";
        if ( args.length != 0 ) {
            mode = args[0];
        }
        app.start(mode);
    }

    /**
     * Start by creating Spark Session
     */
    public void start(String mode) {
        // create a new spark session
        sparkSession = SparkSession.builder()
                .appName("US Birth Rates by County")
                .master("local")
                .getOrCreate();

        // ingest the Birth Rates Data
        String filename = "data/Teen_Birth_Rates_for_Age_Group_15-19_in_the_US_by_County.csv";
        Dataset<Row> initialBirthRateDataFrame = sparkSession.read().format("csv")
                .option("header", "true")
                .load(filename);

        // create a copy of initalBirthRateDataFrame
        Dataset<Row> birthRateDataFrame = initialBirthRateDataFrame;

        // show 15 rows of birthRateDataFrame and schema thus far
        System.out.println("*** Show BirthRateDataFrame and schema thus far ***");
        initialBirthRateDataFrame.show(15);
        initialBirthRateDataFrame.printSchema();

        /**
         * Create Dataframe subset with birthrates greater than 50% and show results
         */
        Dataset<Row> birthRatesGreaterThanFiftyDF = initialBirthRateDataFrame.select(
                "Birth Rate", "State", "County").filter(functions.col("Birth Rate")
                .$greater("50"));
        System.out.println("*** Show Birth Rates Greater than 50% ***");
        birthRatesGreaterThanFiftyDF.show();


        // loop to increase size of initialBirthRateDataFrame
        for ( int i = 0; i < 60; i++ ) {
            birthRateDataFrame = birthRateDataFrame.union(initialBirthRateDataFrame);
        }

        // rename Confidence Limit columns
        birthRateDataFrame = birthRateDataFrame.withColumnRenamed("Lower Confidence Limit", "LCL");
        birthRateDataFrame = birthRateDataFrame.withColumnRenamed("Upper Confidence Limit", "UCL");

    }

}
