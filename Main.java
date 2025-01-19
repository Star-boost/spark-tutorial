package com.virtualpairprogrammers;

import java.util.List;
import java.util.Arrays;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("s3://akindu-spark-demos/input.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(setence -> setence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        List<Tuple2<Long, String>> results = sorted.take(10);

        results.forEach(System.out::println);

        sc.close();
    }
}