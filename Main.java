package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        // dataset.show();

        // long numberOfRows = dataset.count();
        // System.out.println("There are " + numberOfRows + " records");

        // Row firstRow = dataset.first();

        // TASK: get a specific column value from a row

        // option 1: get
        // .get gets the column
        // String subject = firstRow.get(2).toString();

        // option 2: getAs gets the column by name
        // String subject = firstRow.getAs("subject").toString();
        // System.out.println(subject);

        // int year = Integer.parseInt(firstRow.getAs("year"));
        // System.out.println("The year was " + year);

        // TASK: filter just modern art subject

        // option 1: filter with string (sql like)
        // Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND
        // year >= 2007");

        // option 2: lambda expressions as before
        // had to cast function to be a FilterFunction<Row>
        // Dataset<Row> modernArtResults = dataset
        // .filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern
        // Art")
        // && Integer.parseInt(row.getAs("year")) >= 2007);

        // option 3: programmatically (use column class)
        // Column subjectColumn = dataset.col("subject");
        // Column yearColumn = dataset.col("year");
        // Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern
        // Art").and(yearColumn.geq(2007)));

        // another way to retrieve the columns instead of instantiating a new Column
        // instance each time is by using the functions class
        // used a static import to avoid using "functions" prefix
        // Column subjectColumn = col("subject");
        // Column yearColumn = col("year");

        Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
        modernArtResults.show();
        spark.close();

    }
}