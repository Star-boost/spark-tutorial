package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        // what if we want to store both original data and square root data in an rdd?
        // option 1: declare a new class that contains both data
        // option 2: use scala tuples

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        // option 1:
        // JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new
        // IntegerWithSquareRoot(value));

        // option 2:
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(
                value -> new Tuple2<>(value, Math.sqrt(value)));

        // can use empty brackets on the right because type can be inferred from the
        // left
        Tuple2<Integer, Double> myValue = new Tuple2<>(9, 3.0);

        sc.close();

    }
}
