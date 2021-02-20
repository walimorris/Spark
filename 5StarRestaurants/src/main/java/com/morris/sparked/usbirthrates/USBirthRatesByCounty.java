package com.morris.sparked.usbirthrates;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.expr;

/**
 * To run this app please start main app driver, enter mode; noop means no operations take place,
 * col adds creates a new column containing the average between the upper and lower confidence
 * levels and duplicate columns of upper confidence level/upper confidence level 2. Full does this
 * all and deletes the newly created columns. There are some actions that are commented out, this
 * is so these actions aren't added to the time to conduct the actions on this dataset. For the
 * purpose of examination of different action and viewing the dataset and schema, you can uncomment
 * these portions.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class USBirthRatesByCounty {
    private SparkSession sparkSession;

    /**
     * Application driver
     * @param args
     */
    public static void main(String[] args) {
        // initialize app and scanner
        USBirthRatesByCounty app = new USBirthRatesByCounty();
        Scanner scanner = new Scanner(System.in);

        // get app mode and close scanner
        System.out.print("Enter Mode[noop, col, full]: ");
        String mode = scanner.nextLine();
        scanner.close();

        // start app
        app.start(mode);
    }

    /**
     * Start by creating Spark Session
     */
    public void start(String mode) {
        long t0 = System.currentTimeMillis();

        // create a new spark session
        sparkSession = SparkSession.builder()
                .appName("US Birth Rates by County")
                .master("local")
                .getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("1. Creating a session..............." + (t1 - t0));

        // ingest the Birth Rates Data and create initial Dataframe
        String filename = "data/Teen_Birth_Rates_for_Age_Group_15-19_in_the_US_by_County.csv";
        Dataset<Row> initialBirthRateDataFrame = sparkSession.read().format("csv")
                .option("header", "true")
                .load(filename);
        Dataset<Row> birthRateDataFrame = initialBirthRateDataFrame;
        long t2 = System.currentTimeMillis();
        System.out.println("2. Loading initial dataset..............." + (t2 - t1));

//        show 15 rows of birthRateDataFrame and schema thus far
//        System.out.println("*** Show BirthRateDataFrame and schema thus far ***");
//        initialBirthRateDataFrame.show(15);
//        initialBirthRateDataFrame.printSchema();


//        Dataset<Row> birthRatesGreaterThanFiftyDF = initialBirthRateDataFrame.select(
//                "Birth Rate", "State", "County").filter(functions.col("Birth Rate")
//                .$greater("50"));
//        System.out.println("*** Show Birth Rates Greater than 50% ***");
//        birthRatesGreaterThanFiftyDF.show();


        // loop to increase size of initialBirthRateDataFrame
        for ( int i = 0; i < 60; i++ ) {
            birthRateDataFrame = birthRateDataFrame.union(initialBirthRateDataFrame);
        }
        long t3 = System.currentTimeMillis();
        System.out.println("3. Building full dataset..............." + (t3 - t2));

        // rename Confidence Limit columns
        birthRateDataFrame = birthRateDataFrame.withColumnRenamed("Lower Confidence Limit", "LCL");
        birthRateDataFrame = birthRateDataFrame.withColumnRenamed("Upper Confidence Limit", "UCL");
        long t4 = System.currentTimeMillis();
        System.out.println("4. Clean-up..............." + (t4 - t3));

        // Skip all transformations if mode is 'noop', otherwise create new columns
        if ( mode.compareToIgnoreCase("noop") != 0 ) {
            birthRateDataFrame = birthRateDataFrame.withColumn("avg", expr("(lcl + ucl) / 2"))
                    .withColumn("lcl2", birthRateDataFrame.col("lcl"))
                    .withColumn("ucl2", birthRateDataFrame.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0) {
                birthRateDataFrame = birthRateDataFrame.drop(birthRateDataFrame.col("avg"))
                        .drop(birthRateDataFrame.col("lcl2"))
                        .drop(birthRateDataFrame.col("ucl2"));
            }
        }
        // performs collect action
        long t5 = System.currentTimeMillis();
        System.out.println("5. Tranformations..............." + (t5 - t4));

        birthRateDataFrame.collect();
        long t6 = System.currentTimeMillis();
        System.out.println("6. Final Action..............." + (t6 - t5));
        System.out.println("\n# of records..............." + birthRateDataFrame.count());
    }
}
