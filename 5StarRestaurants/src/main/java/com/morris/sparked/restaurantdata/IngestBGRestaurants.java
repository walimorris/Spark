package com.morris.sparked.restaurantdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestBGRestaurants {

    /**
     * Using main as application driver.
     * @param args
     */
    public static void main(String[] args) {
        IngestBGRestaurants app = new IngestBGRestaurants();
        app.start();
    }

    /**
     * Builds a Spark Session on the local master, ingests data from bgrestaurants.csv
     * into a Spark dataframe and shows restaurants schema.
     */
    public void start() {
        // creates session onto the local master
        SparkSession sparkSession = SparkSession.builder()
                .appName("5 Star Restaurants Sofia")
                .master("local")
                .getOrCreate();

        // Loads data from bgrestaurants.csv to Spark dataframe
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load("data/bgrestaurants.csv");

        df.show(3);
        df.printSchema();
        System.out.println("The number of restaurants in schema: " + df.count());
    }

}
