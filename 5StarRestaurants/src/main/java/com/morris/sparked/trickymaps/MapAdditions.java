package com.morris.sparked.trickymaps;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Utilizes {@link AdditionsMapper} and {@link AdditionsForeach} in order to
 * manipulate a dataset containing simple numbers.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class MapAdditions implements Serializable {
    private SparkSession sparksession;

    /**
     * Application driver
     * @param args
     */
    public static void main(String[] args) {
        MapAdditions app = new MapAdditions();
        app.start();
    }

    /**
     * Implements {@link MapFunction} in order to check a dataframe with a column
     * named numbers. If a number is less than 200, our AdditionMapper adds 100 to
     * it, else the same number is mapped to dataframe row.
     */
    private final class AdditionsMapper implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer call(Row row) throws Exception {
            int number = Integer.parseInt(row.getAs("number"));
            return number > 200 ? number : number + 100;
        }
    }

    /**
     * Implements {@link ForeachFunction} in order to loop through a dataframe with
     * column named numbers. This number is than printed to stdOut.
     */
    private class AdditionsForeach implements ForeachFunction<Row> {
        private static final long serialVersionUID = 2L;
        private int count = 0;

        public void call(Row row) throws Exception {
            System.out.println("1. " + row.getAs("number").toString());
        }
    }

    public void start() {
        /* Initiate new spark session on local */
        sparksession = SparkSession.builder()
                .appName("Addtion Mapper App")
                .master("local")
                .getOrCreate();

        /* Ingest data from numbers.csv and create initial dataframe */
        String filename = "data/numbers.csv";
        Dataset<Row> initialAdditionsDf = sparksession.read().format("csv")
                .option("header", "true")
                .load(filename);

        System.out.println("*** Show Initial Additions Dataframe ***");
        initialAdditionsDf.show();
        System.out.println("\n*** Loop Through Initial Additions Dataframe ***");

        /* Utilize AdditionForeach method to print each number to stdOut */
        initialAdditionsDf.foreach(new AdditionsForeach());

        /* Update initial dataframe utilizing AdditionsMapper and rename column back
           to numbers */
        Dataset<Row> updatedAdditionsDf = initialAdditionsDf.map(
                new AdditionsMapper(), Encoders.INT()).toDF();
        updatedAdditionsDf = updatedAdditionsDf.withColumnRenamed(
                "value", "number");
        System.out.println("\n*** Show Updated Additions Dataframe ***");
        updatedAdditionsDf.show();
        System.out.println("\n*** Loop Updated Additions Dataframe ***");

        /* Utilize AdditionsForeach method to print each number to stdOut */
        updatedAdditionsDf.foreach(new AdditionsForeach());
    }
}
