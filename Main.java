package com.virtualpairprogrammers;

import com.virtualpairprogrammers.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        // Keyword Ranking
        // Given a long subtitle file, we want to generate the top 10 words that occur

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // oneish liner
        sc.textFile("src/main/resources/subtitles/input-spring.txt")
                .map(sentence -> sentence.replaceAll("[^a-z A-Z\\s]", "").toLowerCase())
                .filter(sentence -> sentence.trim().length() > 0)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.trim().length() > 0)
                .filter(word -> Util.isNotBoring(word))
                .mapToPair(word -> new Tuple2<String, Long>(word, 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);

        // // load captions into rdd
        // JavaRDD<String> initialRdd =
        // sc.textFile("src/main/resources/subtitles/input-spring.txt");

        // // remove numbers, punctuation, etc.
        // // need double backslash to escape the escape charater
        // JavaRDD<String> lettersOnlyRdd = initialRdd
        // .map(sentence -> sentence.replaceAll("[^a-z A-Z\\s]", "").toLowerCase());

        // // trim to remove leading and trailing whitespace (unprintable characters,
        // etc)
        // JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence ->
        // sentence.trim().length() > 0);

        // JavaRDD<String> justWords = removedBlankLines
        // .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        // JavaRDD<String> blankWordsRemoved = justWords.filter(word ->
        // word.trim().length() > 0);

        // JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word ->
        // Util.isNotBoring(word));

        // // JavaPairRDD mapping word to the Long 1
        // JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word ->
        // new Tuple2<String, Long>(word, 1L));

        // // reduce by key to get the total word counts for each word
        // JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) ->
        // value1 + value2);

        // // because we can only sort by key
        // // switch the keys and the values

        // JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new
        // Tuple2<Long, String>(tuple._2, tuple._1));

        // JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        // // take returns a regular list
        // List<Tuple2<Long, String>> results = sorted.take(10);
        // results.forEach(System.out::println);

        sc.close();

    }
}
