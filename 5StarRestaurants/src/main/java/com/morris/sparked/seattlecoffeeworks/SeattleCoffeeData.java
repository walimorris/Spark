package com.morris.sparked.seattlecoffeeworks;

import com.morris.sparked.models.Coffee;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * An example of creating a Dataframe and converting that dataframe to a Dataset
 * with Rows, in this case, consisting of rows of {@link Coffee} objects. Data
 * is originally ingested through a .csv file to create our {@link Dataset} or
 * DataFrane. That Dataframe is converted to a Dataset<Coffee> by maping each
 * row in our Dataframe to a {@link Coffee} object by utilizing the {@link MapFunction}.
 * We then show how easy it is to convert that Dataframe back to a Dataset.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class SeattleCoffeeData implements Serializable {
    private static final long serialVersionUID = -1L;
    private SparkSession spark;

    class CoffeeMapper implements MapFunction<Row, Coffee> {
        private static final long serialVersionUID = -2L;

        @Override
        public Coffee call(Row value) throws Exception {
            Coffee coffee = new Coffee();
            coffee.setId(value.getAs("id"));
            coffee.setName(value.getAs("name"));
            coffee.setFarmer(value.getAs("farmer"));
            coffee.setRoastLevel(value.getAs("roast_level"));
            coffee.setOrigin(value.getAs("origin"));
            coffee.setLink(value.getAs("link"));
            return coffee;
        }
    }

    /**
     * Application driver
     * @param args
     */
    public static void main(String[] args) {
        SeattleCoffeeData app = new SeattleCoffeeData();
        app.start();
    }

    public void start() {
        // create spark session on local
        spark = SparkSession.builder()
                .appName("Seattle Coffee Data")
                .master("local")
                .getOrCreate();

        // ingest coffee data
        String filename = "data/coffee.csv";
        Dataset<Row> dataframe = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);

        // show dataframe and schema
        System.out.println("*** Coffee Info as dataframe ***");
        dataframe.show(5);
        dataframe.printSchema();

        // convert the coffee dataframe to a dataset
        Dataset<Coffee> dataset = dataframe.map(
                new CoffeeMapper(), Encoders.bean(Coffee.class));
        System.out.println("\n*** Coffee Info as dataset ***");
        dataset.show(5);
        dataset.printSchema();

        // Shows how to create a new sub dataset from filtering. 
        System.out.println("Filter dataset to only show coffee from farmer Geremewe Addisu");
        Dataset<Coffee> farmerset = dataset.filter(dataset.col("farmer").contains("Geremewe Addisu"));
        farmerset.show();
        farmerset.printSchema();

        // Shows how to easily convert coffee dataset back to a dataframe
        Dataset<Row> dataframe2 = dataset.toDF();
        System.out.println("\n ***Converting back to a Dataframe ***");
        dataframe2.show(5);
        dataframe2.printSchema();
    }
}
