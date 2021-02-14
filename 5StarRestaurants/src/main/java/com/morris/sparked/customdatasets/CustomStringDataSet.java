package com.morris.sparked.customdatasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Shows the process of building a Dataset of type {@link String}. This is
 * a different process of building a Dataset of type {@link Row}, which we
 * refer to as a Dataframe. To finish, this custom string dataset will be
 * converted to a dataframe.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class CustomStringDataSet {
    private SparkSession spark;

    /**
     * Application's program driver
     * @param args
     */
    public static void main(String[] args) {
        CustomStringDataSet app = new CustomStringDataSet();
        app.start();
    }

    public void start() {
        /**
         * Build {@link SparkSession} from local
         */
        spark = SparkSession.builder()
                .appName("Custom String Dataset")
                .master("local")
                .getOrCreate();

        // Builds custom string dataset
        String[] customString = {"Seattle", "Cincinnati", "Chicago", "Tokyo"};
        List<String> customStringData = Arrays.asList(customString);
        Dataset<String> dataset = spark.createDataset(customStringData, Encoders.STRING());

        // show visual of dataset string
        System.out.println("Showing custom String dataset visual and schema.");
        dataset.show();
        dataset.printSchema();

        // convert dataset string to dataframe and show visual
        System.out.println("\nShowing custom String dataset converted to dataframe visual and schema.");
        Dataset<Row> dataframe = dataset.toDF();
        dataframe.show();
        dataframe.printSchema();
    }
}
