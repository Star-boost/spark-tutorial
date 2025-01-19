package com.virtualpairprogrammers;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestingJoins {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<Integer, Integer>(4, 18));
        visitsRaw.add(new Tuple2<Integer, Integer>(6, 4));
        visitsRaw.add(new Tuple2<Integer, Integer>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<Integer, String>(1, "John"));
        usersRaw.add(new Tuple2<Integer, String>(2, "Bob"));
        usersRaw.add(new Tuple2<Integer, String>(3, "Alan"));
        usersRaw.add(new Tuple2<Integer, String>(4, "Doris"));
        usersRaw.add(new Tuple2<Integer, String>(5, "Marybelle"));
        usersRaw.add(new Tuple2<Integer, String>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // Inner JOIN:
        // .join is a left join. "discards" entries that aren't common to the 2 rdd's
        // Most commonly used in practical purposes
        // JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);

        // TASK: left outer join the two tables and print all the names UPPER CASE:

        // LEFT outer JOIN:
        // .leftOuterJoin is more similar to sql joins. using all elements of the first
        // rdd, join with the other table
        // if the first table entry isn't present in the second, use Optional.empty
        // JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd =
        // visits.leftOuterJoin(users);

        // .get() extracts the value from an Optional, but not recommended because it
        // can lead to null pointer exceptions still
        // joinedRdd.collect().forEach(it ->
        // System.out.println(it._2._2.get().toUpperCase()));

        // .orElse takes in an "other" parameter which will be printed if there is no
        // value present
        // Otherwise continue calling methods because we know a string is present in
        // that case
        // joinedRdd.collect().forEach(it ->
        // System.out.println(it._2._2.orElse("blank").toUpperCase()));

        // TASK: Print ALL users from the second table and the number of visits they
        // had:

        // RIGHT outer JOIN:
        // Use the second rdd's elements this time and join:
        // JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd =
        // visits.rightOuterJoin(users);

        // joinedRdd.collect()
        // .forEach(it -> System.out.println(" user " + it._2._2 + " had " +
        // it._2._1.orElse(0) + " visits."));

        // FULL outer JOIN:
        // Combination of left outer and right outer joins.
        // Note that either side of the (visits, name) tuple could be OPTIONAL

        // JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedRdd =
        // visits.fullOuterJoin(users);

        // CARTESIAN JOIN:
        // All entires of the first table paired up with all entries from the second
        // table
        // ((4, 18), (1, John)), ((4, 18), (2, Bob)), ... ((10, 9), (6, Raquel))

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(users);

        joinedRdd.collect().forEach(System.out::println);

        sc.close();
    }
}
