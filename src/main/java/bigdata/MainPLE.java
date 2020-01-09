package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import java.util.Arrays;

public class MainPLE {

	//private static final String PHASES_FILE_URL = "/user/gnedelec001/phases1Go.csv";
	private static final String PHASES_FILE_URL = "/user/gnedelec001/phases10Go.csv";
	//private static final String PHASES_FILE_URL = "/raw_data/ALCF_repo/phases.csv";
	private static final String JOBS_FILE_URL = "/raw_data/ALCF_repo/jobs.csv";
	private static final String PATTERNS_FILE_URL = "/raw_data/ALCF_repo/patterns.csv";

	private static final String[] argsPossibilities= {"1a", "1b", "1c", "2a", "3a", "4a", "4b", "5", "6a", "6b", "7"};
	private static String[] target_patterns;

	/**
	 * Display distribution ( Minimum, Maximum, Moyenne, Médiane, premier quadrants, quatrième quadrants, histogramme )
	 */
	private static void displayDistribution(StatCounter statCounter) {
		//Minimum, Maximum, Moyenne, Médiane, premier quadrants, quatrième quadrants, histogramme
		System.out.println("Count:    " + statCounter.count());
		System.out.println("Min:      " + statCounter.min());
		System.out.println("Max:      " + statCounter.max());
		System.out.println("Sum:      " + statCounter.sum());
		System.out.println("Mean:     " + statCounter.mean());
		System.out.println("Variance: " + statCounter.variance());
		System.out.println("Stdev:    " + statCounter.stdev());
	}

	/**
	 * Return true if patterns column == -1
	 */
	private static Function<Tuple2<String, String>,  Boolean> filterIDLE = new Function<Tuple2<String, String>,  Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) throws Exception {
			String[] patterns = data._2().split("-")[1].split(",");
			return Arrays.asList(patterns).contains("-1");
		}
	};

	/**
	 * Return true if patterns column != -1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterNotIDLE = new Function<Tuple2<String, String>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) throws Exception {
			String[] patterns = data._2().split("-")[1].split(",");
			return !Arrays.asList(patterns).contains("-1");
		}
	};

	/**
	 * Return true if pattern columns contains at least 1 pattern from patterns in arguments
	 */
	private static Function<Tuple2<String, String>, Boolean> filterMatchingPatterns = new Function<Tuple2<String, String>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) throws Exception {
			String[] patterns = data._2().split("-")[1].split(",");
			boolean isMatching = false;
			int i = 0;
			while(!isMatching && i < target_patterns.length) {
				String currentPattern = target_patterns[i];
				if(Arrays.asList(patterns).contains(currentPattern)) {
					isMatching = true;
				} else {
					i++;
				}
			}
			return isMatching;
		}
	};


	/**
	 * Question 1.a
	 */
	private static void getDistribOfDurationNotIDLE(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filtered = data.filter(filterNotIDLE);

		JavaDoubleRDD doubleForStats = filtered.mapToDouble(new DoubleFunction<Tuple2<String, String>>() {
			@Override
			public double call(Tuple2<String, String> stringLongTuple2) throws Exception {
				String duration = stringLongTuple2._2().split("-")[0];
				return Double.parseDouble(duration);
			}
		});

		System.out.println("NOMBRE DE DONNEES TOTALE : " + data.count());
		System.out.println("NOMBRE DE DONNEES NON IDLE: " + filtered.count());
		StatCounter statCounter = doubleForStats.stats();
		displayDistribution(statCounter);
	}

	/**
	 * Question 1.b (when pattern == -1)
	 */
	private static void getDistribOfDurationIDLE(JavaPairRDD<String, String> data) {}

	/**
	 * Question 1.c (to do on all patterns (22) )
	 * @param pattern targeted pattern
	 */
	private static void getDistribOfDurationAlonePattern(JavaPairRDD<String, String> data, String pattern) {}

	/**
	 * Question 2.a
	 */
	private static void getDistribOfNbPatternsPerPhase(JavaPairRDD<String, String> data) {}

	/**
	 * Question 3.a
	 */
	private static void getDistribOfNbJobsPerPhase(JavaPairRDD<String, String> data) {}

	/**
	 * Question 4.a
	 */
	private static void getDistribOfTotalPFSAccessPerJob(JavaPairRDD<String, String> data) {}

	/**
	 * Question 4.b
	 */
	private static void getTop10JobsTotalPFSAccess(JavaPairRDD<String, String> data) {}

	/**
	 * Question 5
	 */
	private static void getTotalDurationIDLE(JavaPairRDD<String, String> data) {}

	/**
	 * Question 6.a
	 */
	private static void getDistribOfTotalTimeWithAPatternAlone(JavaPairRDD<String, String> data) {}

	/**
	 * Question 6.b
	 */
	private static void getTop10patterns(JavaPairRDD<String, String> data) {}

	/**
	 * Question 7
	 */
	private static void getLinesMatchingWithPatterns(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filtered = data.filter(filterMatchingPatterns);
		System.out.println("---------------NOMBRE DE DONNEES: " + data.count());
		System.out.println("---------------NOMBRE DE DONNEES FILTREE: " + filtered.count());
	}

	/**
	 * Main program
	 * @param args program's arguments
	 */
	public static void main(String[] args) {
		if (args.length > 0 && Arrays.asList(argsPossibilities).contains(args[0])) {
			SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
			JavaSparkContext context = new JavaSparkContext(conf);
			JavaRDD<String[]> rdd = context.textFile(PHASES_FILE_URL).map(line -> line.split(";"));

			// Mapping lines with :
			// KEY=timestamp_start-timestamp_end
			// VALUE = duration-patterns-npatterns-jobs-njobs-days-ndays
			JavaPairRDD<String, String> data = rdd.mapToPair(s -> new Tuple2<String, String>(s[0] + "-" + s[1], s[2] + "-" + s[3] + "-" + s[4] + "-" + s[5] + "-" + s[6] + "-" + s[7] + "-" + s[8]));

			switch (args[0]) {
				case "1a":
					getDistribOfDurationNotIDLE(data);
					break;
				case "1b":
					getDistribOfDurationIDLE(data);
					break;
				case "1c":
					//Foreach pattern
					for (int i = 0; i < 22; i++) {
						getDistribOfDurationAlonePattern(data, Integer.toString(i));
					}
					break;
				case "2a":
					getDistribOfNbPatternsPerPhase(data);
					break;
				case "3a":
					getDistribOfNbJobsPerPhase(data);
					break;
				case "4a":
					getDistribOfTotalPFSAccessPerJob(data);
					break;
				case "4b":
					getTop10JobsTotalPFSAccess(data);
					break;
				case "5":
					getTotalDurationIDLE(data);
					break;
				case "6a":
					getDistribOfTotalTimeWithAPatternAlone(data);
					break;
				case "6b":
					getTop10patterns(data);
					break;
				case "7":
					if (args.length != 5) {
						System.out.println("Expected exactly 4 patterns in arguments (after the first argument). " + (args.length - 1) + " argument(s) given.");
					} else {
						target_patterns = new String[]{args[1], args[2], args[3], args[4]};
						getLinesMatchingWithPatterns(data);
					}
					break;
				default: // Unreachable
					System.out.println("Your argument " + args[0] + " does not match to any question in the project.");
					System.out.println("Valid arguments : " + String.join(" | ", argsPossibilities));
					break;
			}
			System.out.println("---------------NOMBRE DE DONNEES RDD : " + rdd.getNumPartitions());
			System.out.println("---------------NOMBRE DE PARTITIONS RDD : " + rdd.getNumPartitions());
			context.close();
		} else {
			System.out.println("Expected at least 1 argument matching with : " + String.join(" | ", argsPossibilities));
		}
	}
}
