package com.morris.sparked.restaurantdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * {@link IngestBGRestaurants} builds two distinct {@link Dataset} consisting of restaurants
 * from two different cities in Bulgaria(Sofia/Varna). The two Datasets are then combined by
 * using a sql-like union method. Reporting visuals are presented through STD OUT.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class IngestBGRestaurants {
    private SparkSession sparkSession;

    /**
     * Using main as application driver.
     * @param args
     */
    public static void main(String[] args) {
        IngestBGRestaurants app = new IngestBGRestaurants();
        app.start();
    }

    /**
     * Builds a Spark Session on the local master, ingests data from sofiarestaurants.csv
     * and varnarestaurants.csv into a distinct Spark dataframes and shows both restaurant
     * schema.
     */
    public void start() {
        // creates session onto the local master
        sparkSession = SparkSession.builder()
                .appName("5 Star Restaurants BG")
                .master("local")
                .getOrCreate();
        Dataset<Row> sofiaDF = buildSofiaRestaurantDataFrame();
        Dataset<Row> varnaDF = buildVarnaRestaurantDataFrame();
        Dataset<Row> bulgariaDF = buildBulgariaDataFrame(sofiaDF, varnaDF);

        // displays the individual Dataframes

        // show sofia restaurant schema and db
        sofiaDF.show(3);
        sofiaDF.printSchema();
        System.out.println("The number of restaurants in sofia data frame: " + sofiaDF.count());

        // show varna restaurant schema and db
        varnaDF.show(3);
        varnaDF.printSchema();
        System.out.println("The number of restaurants in varna data frame: " + varnaDF.count());

        // show the final list of Bulgarian restaurants
        bulgariaDF.show(6);
        bulgariaDF.printSchema();
        System.out.println("The number of Bulgarian Restaurants from a union of sofia and varna: " +
                bulgariaDF.count());

    }

    /**
     * Builds sofia restaurant {@link Dataset}
     * @return : {@link Dataset} consisting of restaurants in Sofia, Bulgaria
     */
    private Dataset<Row> buildSofiaRestaurantDataFrame() {
        // Loads data from sofiarestaurants.csv to Spark dataframe
        Dataset<Row> sofiaDF = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load("data/sofiarestaurants.csv");

        // Transforming sofiaDF column names
        sofiaDF = sofiaDF.withColumn("city", lit("sofia"))
                .withColumnRenamed("rname", "restaurant_name")
                .withColumnRenamed("raddress", "restaurant_address")
                .withColumnRenamed("rtype", "cuisine_type")
                .withColumnRenamed("star", "rating");
        return sofiaDF;
    }

    /**
     * Builds Varna restaurant {@link Dataset}
     * @return : {@link Dataset} consisting of restaurants in Varna, Bulgaria
     */
    private Dataset<Row> buildVarnaRestaurantDataFrame() {
        // loads data from varnarestaurants.csv to Spark dataframe
        Dataset<Row> varnaDF = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load("data/varnarestaurants.csv");

        // Transforming varnaDF column names
        varnaDF = varnaDF.withColumn("city", lit("varna"))
                .withColumnRenamed("rname", "restaurant_name")
                .withColumnRenamed("raddress", "restaurant_address")
                .withColumnRenamed("rtype", "cuisine_type")
                .withColumnRenamed("stars", "rating");
        return varnaDF;
    }

    /**
     * Conducts a join on two separate {@link Dataset} restaurants from Sofia and Varna.
     * It makes sense to transform different Datasets to match columns by name and count,
     * which makes for clean data and less errors.
     * @param sofia {@link Dataset} of restaurants from Sofia, Bulgaria
     * @param varna {@link Dataset} of restaurants from Varna, Bulgaria
     * @return {@link Dataset} joining both Sofia and Varna Dataset.
     */
    private Dataset<Row> buildBulgariaDataFrame(Dataset<Row> sofia, Dataset<Row> varna) {
        return sofia.unionByName(varna);
    }
}
