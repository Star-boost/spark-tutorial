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

        // Dataset<Row> modernArtResults = dataset.filter("select * from students where
        // subject = 'Modern Art' AND year >= 2007");

        // we can do full sql syntax

        // we can build an actual table in memory, a "view":
        dataset.createOrReplaceTempView("my_students_table");

        // using the original spark session we built:

        // Some sample sql queries:

        // Dataset<Row> results = spark.sql("select * from my_students_table where
        // subject = 'French'");

        // Dataset<Row> results = spark.sql("select score, year from my_students_table
        // where subject = 'French'");

        // Dataset<Row> results = spark.sql("select max(score) from my_students_table
        // where subject = 'French'");

        // Dataset<Row> results = spark.sql("select avg(score) from my_students_table
        // where subject = 'French'");

        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");

        results.show();
        spark.close();

    }
}