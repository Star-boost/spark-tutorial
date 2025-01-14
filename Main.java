package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        // FlatMaps and Filters

        // given a single input, flatmaps can have 0 or more outputs
        // maps always provide 1 output for each input

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // one-line version:

        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .collect()
                .forEach(System.out::println);

        // JavaRDD<String> sentences = sc.parallelize(inputData);

        // we want to return a java collection when doing split
        // that's why we use asList

        // flatMap also requires an iterator,
        // so we used .iterator()

        // JavaRDD<String> words = sentences.flatMap(value ->
        // Arrays.asList(value.split(" ")).iterator());

        // what if we want to filter out strings of length 1 from our iteratble?
        // use filter:

        // JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);

        // for each iterator print out the words

        // filteredWords.collect().forEach(System.out::println);

        sc.close();

    }
}
