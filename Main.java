package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        /*
         * example of reading and counting different log messages
         * splitting strings like logs is so common spark has a pair rdd
         * they are similar to Maps in Java, except keys can be REPEATED
         */

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // map to pair
        // in the generic, just need to specify the types within the tuples

        /*
         * option 1:
         * group by key pairRdd method maps each key to a Collection containing all
         * values associated by that key
         * can cause a lot of performance issues
         */

        // Using guava Iterables.size method because the collection returned doesn't
        // have an iterable method

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        /*
         * option 2:
         * reduce by key
         * provide a reduction function and the function will be performed across each
         * key
         * if our values are all 1's:
         * our reduction function can add up values for the same key
         * and will return a new rdd with counts for each log type
         */

        // one liner with intermediate rdd's removed, no need to declare the pair rdd's
        // _1 accesses the first element of the tuple, _2 the second

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        sc.close();

    }
}
