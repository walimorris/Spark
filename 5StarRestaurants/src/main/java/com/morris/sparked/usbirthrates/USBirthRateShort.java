package com.morris.sparked.usbirthrates;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;

/**
 * {@link USBirthRateShort} utilizes the 'Teen_Birth_Rates_for_Age_Group...csv' file to conduct
 * short transformations of this dataset. The dataframe is shown at the end, after conducting
 * the final actions. More specifically there should be three new columns: 'avg', 'lcl2' and
 * 'ucl2'. Please run main() to start application.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class USBirthRateShort {
    private SparkSession sparksession;

    /**
     * Create the Application Driver
     */
    public static void main(String[] args) {
        USBirthRateShort app = new USBirthRateShort();
        app.start();
    }

    /**
     * Initialize new spark session, ingest data, conduct transformations, clean-up
     * and initiate action.
     */
    public void start() {
        long t0 = System.currentTimeMillis();
        sparksession = SparkSession.builder().appName("US Birth-rates Short Transformation")
                .master("local")
                .getOrCreate();
        long t1 = System.currentTimeMillis();
        System.out.println("1. Initializing spark session............... " + (t1 - t0));

        // ingest data
        long t2 = System.currentTimeMillis();
        String filename = "data/Teen_Birth_Rates_for_Age_Group_15-19_in_the_US_by_County.csv";
        Dataset<Row> birthratesDf = sparksession.read().format("csv")
                .option("header", "true")
                .load(filename);
        long t3 = System.currentTimeMillis();
        System.out.println("2. Ingesting Initial data............... " + (t3 - t2));

        // conduct clean-up
        long t4 = System.currentTimeMillis();
        birthratesDf = birthratesDf.
                withColumnRenamed("Lower Confidence Limit", "lcl")
                .withColumnRenamed("Upper Confidence Limit", "ucl");
        long t5 = System.currentTimeMillis();
        System.out.println("3. Conducting Clean-up............... " + (t5 - t4));

        // conduct transformations
        long t6 = System.currentTimeMillis();
        birthratesDf = birthratesDf
                .withColumn("avg", expr("lcl+ucl/2"))
                .withColumn("lcl2", birthratesDf.col("lcl"))
                .withColumn("ucl2", birthratesDf.col("ucl"));
        long t7 = System.currentTimeMillis();
        System.out.println("4. Conducting Transformations.............. " + (t7 - t6));

        // conduct action on dataframe
        long t8 = System.currentTimeMillis();
        birthratesDf.collect();
        long birthrateDfCount = birthratesDf.count();
        long t9 = System.currentTimeMillis();
        System.out.println("5. Conducting Actions of Dataframe............... " + (t9 - t8));

        System.out.println("\nResults\n____________________\ncount: " + birthrateDfCount + "\n");

        // show 20 rows to confirm transformations
        birthratesDf.show(20);
    }
}
