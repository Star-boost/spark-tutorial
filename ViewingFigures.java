package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing
 * figures.
 * You can ignore until then.
 */
public class ViewingFigures {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;

		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!

		/*
		 * WARMUP:
		 * build an rdd containing a key of courseId together with the number of
		 * chapters on that course
		 */
		// JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData
		// .mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
		// .reduceByKey((value1, value2) -> value1 + value2);

		// chapterCountRdd.collect().forEach(System.out::println);

		/*
		 * MAIN EXERCISE:
		 * produce a ranking chart of the most popular courses, use our
		 * "Weighted Course View Ratio Process"
		 */

		// Step 1 - remove any duplicated views
		viewData = viewData.distinct();

		// Step 2 - Joining to get courseId in the RDD
		JavaPairRDD<Integer, Integer> switchedViewData = viewData
				.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, tuple._1));

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedChapterToUserAndCourse = switchedViewData
				.join(chapterData);

		// Step 3 - Drop the chapter Id
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> userAndCourseToViewTally = joinedChapterToUserAndCourse
				.mapToPair(tuple -> new Tuple2<>(tuple._2, 1));

		// Step 4 - Count Views for User/Course
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> userAndCourseToViewSummed = userAndCourseToViewTally
				.reduceByKey((value1, value2) -> value1 + value2);

		// Step 5 - Drop the userId
		JavaPairRDD<Integer, Integer> courseToViewRdd = userAndCourseToViewSummed
				.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._1._2, tuple._2));

		// Step 6 - of how many chapters?
		JavaPairRDD<Integer, Integer> switchedChapterData = chapterData
				.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, tuple._1));

		JavaPairRDD<Integer, Integer> switchedChapterDataTally = switchedChapterData
				.mapToPair(tuple -> new Tuple2<>(tuple._1, 1));

		JavaPairRDD<Integer, Integer> switchedChapterDataSummed = switchedChapterDataTally
				.reduceByKey((value1, value2) -> value1 + value2);

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseToViewAndChapterLengthRdd = courseToViewRdd
				.join(switchedChapterDataSummed);

		// Step 7 - Convert to percentages
		// had to cast elements to double
		// can also use mapValues function which we can use a lambda function on just
		// the values (so key doesn't change)
		JavaPairRDD<Integer, Double> courseToPercentageRdd = courseToViewAndChapterLengthRdd
				.mapToPair(tuple -> new Tuple2<>(tuple._1, ((double) tuple._2._1 / (double) tuple._2._2)));

		// Step 8 - Convert to scores
		JavaPairRDD<Integer, Integer> courseToScoreTally = courseToPercentageRdd
				.mapToPair(tuple -> new Tuple2<>(tuple._1,
						tuple._2 > 0.9 ? 10 : tuple._2 > 0.5 ? 4 : tuple._2 > 0.25 ? 2 : 0));

		// Step 9 - Add up the total scores
		JavaPairRDD<Integer, Integer> courseToScoreSummedRdd = courseToScoreTally
				.reduceByKey((value1, value2) -> value1 + value2);

		// Exercise 3: Get titles for the courses:
		JavaPairRDD<Integer, Tuple2<String, Integer>> courseIdToCourseNameAndScoreSummed = titlesData
				.join(courseToScoreSummedRdd);
		JavaPairRDD<Integer, String> summedScoreToCourseNameRdd = courseIdToCourseNameAndScoreSummed
				.mapToPair(tuple -> new Tuple2<>(tuple._2._2, tuple._2._1)).sortByKey(false);

		// consider using Long next time for any value that might exceed 2 billion (like
		// summed views, for example)

		summedScoreToCourseNameRdd.collect().forEach(System.out::println);
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] cols = commaSeparatedLine.split(",");
					return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
				});
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96, 1));
			rawChapterData.add(new Tuple2<>(97, 1));
			rawChapterData.add(new Tuple2<>(98, 1));
			rawChapterData.add(new Tuple2<>(99, 2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] cols = commaSeparatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
				});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

		if (testMode) {
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return sc.parallelizePairs(rawViewData);
		}

		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] columns = commaSeparatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				});
	}
}
