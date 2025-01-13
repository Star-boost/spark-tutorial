package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        // create the spark config
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        // load to config into a new Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);
        // load in a collection and turn it into an rdd
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        // lambda function argument for reduce
        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        // map
        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

        // foreach takes a voidFunction in
        // so it's different from map
        // sqrtRdd.foreach(value -> System.out.println(value));

        // another way to do this (double colon syntax)
        // notice we had to do collect because println is not serializable
        sqrtRdd.collect().forEach(System.out::println);

        System.out.println(result);

        // how many elements in sqrtRdd?
        System.out.println(sqrtRdd.count());

        // how many elements in sqrtRdd using only map and reduce?
        // put the "L" after 1 to make java treat it as a long
        // we use a long because Integer has a limit of around 2 billion
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println(count);

        sc.close();

    }
}
