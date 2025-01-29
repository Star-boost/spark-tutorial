package com.virtualpairprogrammers;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        // REQUIREMENT - report of error type, month, and total number of that error
        // type for that month

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        // use the date_format function
        // format string from java datetime simple date format class
        // use AS + alias to change the header for the formatted month

        // add in a dummy column with a 1 to do an aggregation based on the two
        // exisiting columns we want to group by
        // do a count aggregation on this new column

        // IMPORTANT: any column that isn't in the group by needs an aggregation
        // function applied to it.

        // Dateformat returns a string.
        // Dataset<Row> results = spark
        // .sql("select level, date_format(datetime, 'MMMM') as month,
        // cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total
        // "
        // +
        // "from logging_table group by level, month order by monthnum");

        // dropping the monthnum column in java
        // results = results.drop("monthnum");

        // another way without creating/dropping the monthnum
        // added order by level as well
        Dataset<Row> results = spark
                .sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");

        results.show(100);

        spark.close();

    }
}