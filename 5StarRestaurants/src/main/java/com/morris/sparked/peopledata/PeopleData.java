package com.morris.sparked.peopledata;

import com.morris.sparked.models.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

import static org.apache.spark.sql.functions.lit;

public class PeopleData implements Serializable {
    public static final long serialVersionUID = -1L;
    private SparkSession spark;

    class PeopleOrganizer implements MapFunction<Row, Person> {

        @Override
        public Person call(Row value) throws Exception {
            Person person = new Person();
            person.setFirstName(value.getAs("first_name"));
            person.setLastName(value.getAs("last_name"));
            person.setAge(value.getAs("age"));
            person.setGender(value.getAs("gender"));
            return person;
        }
    }

    /**
     * Main is Application Driver
     * @param args
     */
    public static void main(String[] args) {
        PeopleData app = new PeopleData();
        app.start();
    }

    /**
     * Start Spark Session on local
     */
    public void start() {
        spark = SparkSession.builder()
                .appName("People Data Review")
                .master("local")
                .getOrCreate();

        // It's time to ingest People Data into a Dataframe
        String filename = "data/people.csv";
        Dataset<Row> peopleDataFrame = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);

        // Show people dataframe and schema
        System.out.println("*** Time to view People Dataframe and Schema ***");
        peopleDataFrame.show();
        peopleDataFrame.printSchema();

        // convert dataframe to data set and show
        Dataset<Person> personDataSet = peopleDataFrame.map(
                new PeopleOrganizer(), Encoders.bean(Person.class));
        System.out.println("*** Time to show People Dataset and Schema ***");
        personDataSet.show();
        personDataSet.printSchema();

        // convert people dataset to dataframe
        Dataset<Row> peopleDataFrame2 = personDataSet.toDF();

        // people dataframe2 column names have changed due to conversion to datset
        // let's fix that and convert the columns back to original
        peopleDataFrame2 = peopleDataFrame2.withColumnRenamed("firstName", "first_name")
                .withColumnRenamed("lastName", "last_name");

        // add a 'profession' column to peopleDataFrame2
        peopleDataFrame2 = peopleDataFrame2.withColumn("profession", lit("writer"));

        // show peopleDataFrame2 with profession column
        System.out.println("*** Time to show People Dataframe2 with professions column added ***");
        peopleDataFrame2.show();

        // update a Persons profession in dataframe2
        // how to change nullable ?
        // why are column out of order?
        Dataset<Row> peopleDataFrameSubset = peopleDataFrame2
                .filter(peopleDataFrame2.col("first_name").contains("wali"))
                .withColumn("profession", lit("programmer"));
        peopleDataFrame2 = peopleDataFrame2
                .filter(peopleDataFrame2.col("first_name").notEqual("wali"));
        peopleDataFrame2 = peopleDataFrameSubset.unionByName(peopleDataFrame2);

        // print people dataframe2 and check for update
        System.out.println("*** Time to show People DataFrame2 with updates ***");
        peopleDataFrame2.show();
        peopleDataFrame2.printSchema();
    }
}
