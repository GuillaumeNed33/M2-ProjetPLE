package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class MainPLE {
	public static final String RESET_COLOR = "\033[0m";  // Text Reset
	public static final String COLOR_RED_BACKGROUND = "\033[41m";  // Text Reset

	//private static final String PHASES_FILE_URL = "/user/gnedelec001/phasesHead1Go.csv";
	private static final String PHASES_FILE_URL = "/user/gnedelec001/phasesHead10Go.csv";
	//private static final String PHASES_FILE_URL = "/user/gnedelec001/phasesTail1Go.csv";
	//private static final String PHASES_FILE_URL = "/user/gnedelec001/phasesTail10Go.csv";
	//private static final String PHASES_FILE_URL = "/raw_data/ALCF_repo/phases.csv";
	private static final String JOBS_FILE_URL = "/raw_data/ALCF_repo/jobs.csv";
	private static final String PATTERNS_FILE_URL = "/raw_data/ALCF_repo/patterns.csv";

	private static final String[] argsPossibilities= {"1a", "1b", "1c", "2a", "3a", "4a", "4b", "5", "6a", "6b", "7"};
	private static String[] target_patterns;
	private static String patterns_selected;

	/**
	 * Return true if patterns column == -1
	 */
	private static Function<Tuple2<String, String>,  Boolean> filterIDLE = new Function<Tuple2<String, String>,  Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) throws Exception {
			String[] patterns = data._2().split("/")[1].split(",");
			return Arrays.asList(patterns).contains("-1") && !Arrays.asList(patterns).contains("phases");
		}
	};

	/**
	 * Return true if patterns column != -1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterNotIDLE = new Function<Tuple2<String, String>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) {
			String[] patterns = data._2().split("/")[1].split(",");
			return !Arrays.asList(patterns).contains("-1") && !Arrays.asList(patterns).contains("phases");
		}
	};

	/**
	 * Return true if patterns column != -1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterAlonePattern = new Function<Tuple2<String, String>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) {
			String[] patterns = data._2().split("/")[1].split(",");
			return patterns.length == 1 && patterns[0].equals(patterns_selected);
		}
	};

	/**
	 * Return true if pattern columns contains all patterns from patterns in arguments
	 */
	private static Function<Tuple2<String, String>, Boolean> filterMatchingPatterns = new Function<Tuple2<String, String>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, String> data) {
			String[] patterns = data._2().split("/")[1].split(",");
			boolean allAreMatching = true;
			int i = 0;
			while(allAreMatching && i < target_patterns.length) {
				String currentPattern = target_patterns[i];
				if(!Arrays.asList(patterns).contains(currentPattern)) {
					allAreMatching = false;
				} else {
					i++;
				}
			}
			return allAreMatching;
		}
	};

	/**
	 * Map data to double for stats
	 */
	private static DoubleFunction<Tuple2<String, String>> mappingDurationForStats = new DoubleFunction<Tuple2<String, String>>() {
		@Override
		public double call(Tuple2<String, String> data) throws Exception {
			String duration = data._2().split("/")[0];
			return Double.parseDouble(duration);
		}
	};

	/**
	 * Display distribution ( Minimum, Maximum, Moyenne, Médiane, premier quadrants, troisième quadrants, histogramme )
	 */
	private static void displayDistribution(JavaDoubleRDD data) {
		NumberFormat formatter = new DecimalFormat("0.#E0");
		double min = data.min();
		double max = data.max();
		double mean = data.mean();
		double median = 0;
		double firstQuartile = 0;
		double thirdQuartile = 0;
		double[] intervalsForDuration = {
				1.0,
				1.0E2,
				1.0E3,
				5.0E3,
				1.0E4,
				2.5E4,
				5.0E4,
				7.5E4,
				1.0E5,
				5.0E5,
				1.0E6,
				1.0E7,
				1.0E8,
				2.0E9,
				2.0E10
		};
		long[] values = data.histogram(intervalsForDuration);

		if(data.count() > 0) {
			List<Double> dataList = new ArrayList<>(data.collect());
			Collections.sort(dataList);

			int indexMedian = (data.count() % 2 == 0) ? ((int) (data.count() / 2)-1) : (int) (((data.count() + 1) / 2)-1);
			median = (data.count() % 2 == 0) ? (dataList.get(indexMedian) + dataList.get(indexMedian + 1)) / 2 : dataList.get(indexMedian);

			int oneQuarter = (int) Math.ceil(data.count() / 4.); // Calculate the number of value in one quarter
			int threeQuarter = (int) Math.ceil(3 * data.count() / 4.); // Calculate the number of value in three quarter
			firstQuartile = dataList.get(oneQuarter); // Max value of 25% of dataset
			thirdQuartile = dataList.get(threeQuarter); // Max value of 75% of dataset
		}
		System.out.println(COLOR_RED_BACKGROUND + "Minimum:      " + min + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Maximum:      " + max + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Moyenne:     " + mean + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Médiane:     " + median + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Premier quartile:     " + firstQuartile + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Troisième quartile:     " + thirdQuartile + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Histogramme (microsecondes):	" + RESET_COLOR);
		for(int i=0; i < values.length; i++) {
			System.out.println(COLOR_RED_BACKGROUND + "Entre " + formatter.format(intervalsForDuration[i]) + " et " + formatter.format(intervalsForDuration[i+1]) + " :		" + values[i] + RESET_COLOR);
		}
	}

	/**
	 * Question 1.a
	 */
	private static void getDistribOfDurationNotIDLE(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);

		System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DES DUREES DES PHASES NON IDLE ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1) + RESET_COLOR);
		displayDistribution(dataForStats);
	}

	/**
	 * Question 1.b (pattern == -1)
	 */
	private static void getDistribOfDurationIDLE(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);

		System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DES DUREES DES PHASES IDLE ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1) + RESET_COLOR);
		displayDistribution(dataForStats);
	}

	/**
	 * Question 1.c
	 */
	private static void getDistribOfDurationAlonePattern(JavaPairRDD<String, String> data) {
		Map<String, JavaPairRDD<String, String>> allFilteredData = new TreeMap<>();
		Map<String, JavaDoubleRDD> allDataForStats = new TreeMap<>();
		for (int i = 0; i < 22; i++) {
			patterns_selected = Integer.toString(i); //Save the pattern to allow access to it  in filterAlonePattern method
			JavaPairRDD<String, String> filteredData = data.filter(filterAlonePattern);
			JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);
			allFilteredData.put(Integer.toString(i), filteredData);
			allDataForStats.put(Integer.toString(i), dataForStats);
		}

		for (int i = 0; i < 22; i++) {
			System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DES DUREES OU LE PATTERN " + i + " APPARAIT SEUL ---" + RESET_COLOR);
			System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + allFilteredData.get(Integer.toString(i)).count() + " sur " + (data.count()-1) + RESET_COLOR);
			if(allFilteredData.get(Integer.toString(i)).count() > 0) {
				displayDistribution(allDataForStats.get(Integer.toString(i)));
			}
		}
	}

	/**
	 * Question 2.a
	 */
	private static void getDistribOfNbPatternsPerPhase(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DES NPATTERNS DES PHASES NON IDLE ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1) + RESET_COLOR);
	}

	/**
	 * Question 3.a
	 */
	private static void getDistribOfNbJobsPerPhase(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DES NJOBS DES PHASES NON IDLE ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1) + RESET_COLOR);
	}

	/**
	 * Question 4.a
	 */
	private static void getDistribOfTotalPFSAccessPerJob(JavaPairRDD<String, String> data) {
		System.out.println(COLOR_RED_BACKGROUND + "--- DISTRIBUTION DU TEMPS TOTAL D'ACCES AU PFS PAR JOB ---" + RESET_COLOR);
	}

	/**
	 * Question 4.b
	 */
	private static void getTop10JobsTotalPFSAccess(JavaPairRDD<String, String> data) {
		System.out.println(COLOR_RED_BACKGROUND + "--- TOP 10 DES JOBS EN TEMPS TOTAL D'ACCES AU PFS ---" + RESET_COLOR);
	}

	/**
	 * Question 5
	 */
	private static void getTotalDurationIDLE(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);
		System.out.println(COLOR_RED_BACKGROUND + "--- TEMPS TOTAL EN IDLE DU SYSTEME ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Resultat en microsecondes:	" + dataForStats.sum() + RESET_COLOR);
	}

	/**
	 * Question 6.a
	 */
	private static void getDistribOfTotalTimeWithAPatternAlone(JavaPairRDD<String, String> data) {
		System.out.println(COLOR_RED_BACKGROUND + "--- POURCENTAGE DU TEMPS TOTAL DES PHASES OU CHAQUE PATTERN A ETE OBSERVE SEUL OU CONCURRENT A DES AUTRES ---" + RESET_COLOR);
	}

	/**
	 * Question 6.b
	 */
	private static void getTop10patterns(JavaPairRDD<String, String> data) {
		System.out.println(COLOR_RED_BACKGROUND + "--- TOP 10 DES PATTERNS EN REPRESENTATIVITE ---" + RESET_COLOR);
	}

	/**
	 * Question 7
	 */
	private static void getLinesMatchingWithPatterns(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterMatchingPatterns);
		System.out.println(COLOR_RED_BACKGROUND + "--- PLAGES HORAIRES QUI COMPORTENT LES 4 PATTERNS SUIVANTS : " + String.join(",", target_patterns) +" ---" + RESET_COLOR);
		System.out.println(COLOR_RED_BACKGROUND + "Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1) + RESET_COLOR);
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
			// KEY = timestamp_start/timestamp_end
			// VALUE = duration/patterns/npatterns/jobs/njobs/days/ndays
			JavaPairRDD<String, String> data = rdd.mapToPair(s ->
					new Tuple2<>(
							s[0] + "/" + s[1],
							s[2] + "/" + s[3] + "/" + s[4] + "/" + s[5] + "/" + s[6] + "/" + s[7] + "/" + s[8]
					)
			);

			switch (args[0]) {
				case "1a":
					getDistribOfDurationNotIDLE(data);
					break;
				case "1b":
					getDistribOfDurationIDLE(data);
					break;
				case "1c":
					getDistribOfDurationAlonePattern(data);
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
			context.close();
		} else {
			System.out.println("Expected at least 1 argument matching with : " + String.join(" | ", argsPossibilities));
		}
	}
}
