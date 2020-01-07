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

	/**
	 * Question 1.a
	 */
	private static void getDistribOfDurationNotIDLE() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String[]> rdd = context.textFile(PHASES_FILE_URL).map(line -> line.split(";"));
		JavaPairRDD<String, String> rddp = rdd.mapToPair(s -> new Tuple2<String, String>(s[0] + "-" + s[1], s[2] + "-" + s[3]));

		JavaPairRDD<String, String> filtered = rddp.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> stringTuple) throws Exception {
				String[] patterns = stringTuple._2().split(",");
				boolean notIDLE = true;
				int i = 0;
				while(notIDLE && i < patterns.length) {
					String currentPattern = patterns[i];
					if(currentPattern.equals("-1")) {
						notIDLE = false;
					} else {
						i++;
					}
				}
				return notIDLE;
			}
		});

		JavaDoubleRDD doubleForStats = filtered.mapToDouble(new DoubleFunction<Tuple2<String, String>>() {
			@Override
			public double call(Tuple2<String, String> stringLongTuple2) throws Exception {
				String duration = stringLongTuple2._2().split("-")[0];
				return Double.parseDouble(duration);
			}
		});

		StatCounter statCounter = doubleForStats.stats();

		System.out.println("NOMBRE DE DONNEES TOTALE : " + rdd.count());
        System.out.println("NOMBRE DE DONNEES NON IDLE: " + filtered.count());
        System.out.println("NOMBRE DE PARTITIONS : " + rdd.getNumPartitions());

        System.out.println("Using StatCounter");
        System.out.println("Count:    " + statCounter.count());
        System.out.println("Min:      " + statCounter.min());
        System.out.println("Max:      " + statCounter.max());
        System.out.println("Sum:      " + statCounter.sum());
        System.out.println("Mean:     " + statCounter.mean());
        System.out.println("Variance: " + statCounter.variance());
        System.out.println("Stdev:    " + statCounter.stdev());
	}

	/**
	 * Question 1.b (when pattern == -1)
	 */
	private static void getDistribOfDurationIDLE() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 1.c (to do on all patterns (22) )
	 * @param pattern targeted pattern
	 */
	private static void getDistribOfDurationAlonePattern(String pattern) {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 2.a
	 */
	private static void getDistribOfNbPatternsPerPhase() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 3.a
	 */
	private static void getDistribOfNbJobsPerPhase() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 4.a
	 */
	private static void getDistribOfTotalPFSAccessPerJob() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 4.b
	 */
	private static void getTop10JobsTotalPFSAccess() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 5
	 */
	private static void getTotalDurationIDLE() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 6.a
	 */
	private static void getDistribOfTotalTimeWithAPatternAlone() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 6.b
	 */
	private static void getTop10patterns() {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);
	}

	/**
	 * Question 7
	 * @param target_patterns pattern matching
	 */
	private static void getLinesMatchingWithPatterns(String[] target_patterns) {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String[]> rdd = context.textFile(PHASES_FILE_URL).map(line -> line.split(";"));

		//Map : key = timestamp_debut-timestamp_fin, value=patterns
		JavaPairRDD<String, String> rddp = rdd.mapToPair(s -> new Tuple2<String, String>(s[0] + "-" + s[1], s[3]));

		//Get lines matching with the targeted patterns
		JavaPairRDD<String, String> filtered = rddp.filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> stringTuple) throws Exception {
				String[] patterns = stringTuple._2().split(",");
				boolean matchingPattern = false;
				int i = 0;
				while(!matchingPattern && i < patterns.length) {
					String currentPattern = patterns[i];
					if(Arrays.asList(target_patterns).contains(currentPattern)) {
						matchingPattern = true;
					} else {
						i++;
					}
				}
				return matchingPattern;
			}
		});
		System.out.println("---------------NOMBRE DE DONNEES: " + rdd.count());
		System.out.println("---------------NOMBRE DE DONNEES FILTREE: " + filtered.count());
		System.out.println("---------------NOMBRE DE PARTITIONS : " + rdd.getNumPartitions());
	}

	/**
	 * Main program
	 * @param args program's arguments
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Expected arguments -- ");
		} else {
			switch (args[0]) {
				case "1a":
					getDistribOfDurationNotIDLE();
					break;
				case "1b":
					getDistribOfDurationIDLE();
					break;
				case "1c":
					//Foreach pattern
					for(int i = 0; i < 22; i++) {
						getDistribOfDurationAlonePattern(Integer.toString(i));
					}
					break;
				case "2a":
					getDistribOfNbPatternsPerPhase();
					break;
				case "3a":
					getDistribOfNbJobsPerPhase();
					break;
				case "4a":
					getDistribOfTotalPFSAccessPerJob();
					break;
				case "4b":
					getTop10JobsTotalPFSAccess();
					break;
				case "5":
					getTotalDurationIDLE();
					break;
				case "6a":
					getDistribOfTotalTimeWithAPatternAlone();
					break;
				case "6b":
					getTop10patterns();
					break;
				case "7":
					if(args.length != 5) {
						System.out.println("Expected exactly 4 patterns in arguments (after the first argument). " + (args.length-1) + " argument(s) given.");
					} else {
						String[] patterns = {args[1], args[2], args[3], args[4]};
						getLinesMatchingWithPatterns(patterns);
					}
					break;
				default:
					System.out.println("Your argument " + args[0] + " does not match to any question in the project.");
					System.out.println("Valid arguments : 1a | 1b | 1c | 2a | 3a | 4a | 4b | 5 | 6a | 6b | 7");
					break;
			}
		}
	}
}
