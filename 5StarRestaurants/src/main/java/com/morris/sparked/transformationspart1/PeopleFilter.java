package com.morris.sparked.transformationspart1;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * This class shows an example of filtering people data by age, implementing {@link PeopleFilterByAge}
 * inner class, which implements {@link FilterFunction}.
 *
 * @author wmm<walimmorris@gmail.com>
 */
public class PeopleFilter implements Serializable {
    private SparkSession sparksession;

    /**
     * Implements {@link FilterFunction} in order to check each row's age column in people data. Ages
     * greater than 30 will be filtered out of the results. Column key name must be "age".
     */
    public class PeopleFilterByAge implements FilterFunction<Row> {
        private static final long serialVersionUID = 17392L;

        @Override
        public boolean call(Row row) throws Exception {
            return (Integer.parseInt(row.getAs("age")) < 30);
        }
    }

    /**
     * Implements {@link FilterFunction} in order to filter people by gender, in this case male.
     * Column key name should be 'male', not case sensitive.
     */
    public class PeopleFilterByGenderMale implements FilterFunction<Row> {
        private static final long serialVersionUID = 1234567L;

        @Override
        public boolean call(Row row) throws Exception {
            String gender = row.getAs("gender");
            return (gender.compareToIgnoreCase("male") == 0);
        }
    }

    /**
     * Implements {@link FilterFunction} in order to filter people by gender, in this case female.
     * Column key name should be 'female', not case sensitive.
     */
    public class PeopleFilterByGenderFemale implements FilterFunction<Row> {
        private static final long serialVersionUID = 12345678L;

        @Override
        public boolean call(Row row) throws Exception {
            String gender = row.getAs("gender");
            return (gender.compareToIgnoreCase("female") == 0);
        }
    }

    /**
     * Application driver
     * @param args
     */
    public static void main(String[] args) {
        PeopleFilter app = new PeopleFilter();
        app.start();
    }

    private void start() {
        // initialize new spark session
        sparksession = SparkSession.builder().appName("Filter People by Age")
                .master("local")
                .getOrCreate();

        // ingest people data into a dataframe
        String filename = "data/people.csv";
        Dataset<Row> peopleDf = sparksession.read().format("csv")
                .option("header", "true")
                .load(filename);

        // conduct transformations, in this case filtering people data by age
        System.out.println("*** Filter by age: younger than 30 ***");
        Dataset<Row> peopleByAgeDf = peopleDf.filter(new PeopleFilterByAge());
        peopleByAgeDf.show();

        // filter males
        System.out.println("\n*** Filter by gender: male ***");
        Dataset<Row> peopleMaleDf = peopleDf.filter(new PeopleFilterByGenderMale());
        peopleMaleDf.show();

        // filter females
        System.out.println("\n*** Filter by gender: female ***");
        Dataset<Row> peopleFemaleDf = peopleDf.filter(new PeopleFilterByGenderFemale());
        peopleFemaleDf.show();
    }
}
