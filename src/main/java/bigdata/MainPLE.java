package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.lang.Math;
import java.io.Serializable;

public class MainPLE {
	private static JavaSparkContext context;

	private static final String OUTPUT_URL = "/user/gnedelec001/output/";
	private static final String PHASES_FILE_URL = "/user/gnedelec001/resources/phasesHead1Go.csv";
	//private static final String PHASES_FILE_URL = "/user/gnedelec001/resources/phasesHead10Go.csv";
	//private static final String PHASES_FILE_URL = "/user/gnedelec001/resources/phasesTail1Go.csv";
	//private static final String PHASES_FILE_URL = "/user/gnedelec001/resources/phasesTail10Go.csv";
	//private static final String PHASES_FILE_URL = "/raw_data/ALCF_repo/phases.csv";

	private static final String JOBS_FILE_URL = "/raw_data/ALCF_repo/jobs.csv";
	private static final String PATTERNS_FILE_URL = "/raw_data/ALCF_repo/patterns.csv";

	private static final Integer NUM_PARTITIONS = 1;
	private static final String[] argsPossibilities= {"1a", "1b", "1c", "2", "3", "4", "5", "6", "7"};
	private static String[] target_patterns;
	private static String patterns_selected;

	private static final double[] intervalsForNbPatterns = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
	private static final double[] intervalsForNbJobs = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 1000, 100000};
	private static final double[] intervalsForDuration = {
			0,
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

	/**
	 * Return true if patterns column == -1
	 */
	private static Function<Tuple2<String, String>,  Boolean> filterIDLE = (Function<Tuple2<String, String>, Boolean>) data -> {
		String[] patterns = data._2().split("/")[1].split(",");
		return Arrays.asList(patterns).contains("-1") && !Arrays.asList(patterns).contains("phases"); //Dans le sujet l'entete est nommé "patterns" mais "phases" dans le fichier réel.

	};

	/**
	 * Return true if patterns column != -1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterNotIDLE = (Function<Tuple2<String, String>, Boolean>) data -> {
		String[] patterns = data._2().split("/")[1].split(",");
		return !Arrays.asList(patterns).contains("-1") && !Arrays.asList(patterns).contains("phases"); //Dans le sujet l'entete est nommé "patterns" mais "phases" dans le fichier réel.
	};


	/**
	 * Return true if pattern is present
	 */
	private static Function<Tuple2<String, String>, Boolean> filterPresentPattern = (Function<Tuple2<String, String>, Boolean>) data -> {
		String[] patterns = data._2().split("/")[1].split(",");
		return Arrays.asList(patterns).contains(patterns_selected);
	};

	/**
	 * Return true if pattern = pattern_selected and npattern = 1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterAloneSelectedPattern = (Function<Tuple2<String, String>, Boolean>) data -> {
		String[] patterns = data._2().split("/")[1].split(",");
		return patterns.length == 1 && patterns[0].equals(patterns_selected);
	};

	/**
	 * Return true if patterns contains pattern_selected and patterns length > 1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterMultipleSelectedPattern = (Function<Tuple2<String, String>, Boolean>) data -> {
		String[] patterns = data._2().split("/")[1].split(",");
		return patterns.length > 1 && Arrays.asList(patterns).contains(patterns_selected);
	};

	/**
	 * Return true if patterns column != -1
	 */
	private static Function<Tuple2<String, String>, Boolean> filterAlonePattern = (Function<Tuple2<String, String>, Boolean>) data -> {
		String nbPatterns = data._2().split("/")[2];
		return !nbPatterns.equals("nphases") && Integer.parseInt(nbPatterns) == 1;
	};

	/**
	 * Return true if pattern columns contains all patterns from patterns in arguments
	 */
	private static Function<Tuple2<String, String>, Boolean> filterMatchingPatterns = (Function<Tuple2<String, String>, Boolean>) data -> {
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
	};

	/**
	 * Map data to double for stats : get duration
	 */
	private static DoubleFunction<Tuple2<String, String>> mappingDurationForStats = (DoubleFunction<Tuple2<String, String>>) data -> {
		String duration = data._2().split("/")[0];
		return Double.parseDouble(duration);
	};

	/**
	 * Map data to pair for stats : get duration by pattern
	 */
	private static PairFunction<Tuple2<String, String>, Integer, Double> mappingMultipleDurationForStats = (PairFunction<Tuple2<String, String>, Integer, Double>) data -> {
		String duration = data._2().split("/")[0];
		String pattern = data._2().split("/")[1];
		return new Tuple2<>(Integer.parseInt(pattern), Double.parseDouble(duration));
	};


	/**
	 * Map data to double for stats : get nb patterns
	 */
	private static DoubleFunction<Tuple2<String, String>> mappingNbPatternsForStats = (DoubleFunction<Tuple2<String, String>>) data -> {
		String npatterns = data._2().split("/")[2];
		return Double.parseDouble(npatterns);
	};

	/**
	 * Map data to double for stats : get nb jobs
	 */
	private static DoubleFunction<Tuple2<String, String>> mappingNbJobsForStats = (DoubleFunction<Tuple2<String, String>>) data -> {
		String njobs = data._2().split("/")[4];
		return Double.parseDouble(njobs);
	};

	/**
	 * Display distribution ( Minimum, Maximum, Moyenne, Médiane, premier quadrants, troisième quadrants, histogramme )
	 */
	private static ArrayList<String> displayDistribution(JavaDoubleRDD data, double[] intervals) {
		ArrayList<String> result = new ArrayList<>();
		StatCounter stats = data.stats();
		long size = stats.count();
		double min = stats.min();
		double max = stats.max();
		double mean = stats.mean();
		double median = 0;
		double firstQuartile = 0;
		double thirdQuartile = 0;
		long[] values = data.histogram(intervals);
		NumberFormat formatter = new DecimalFormat("0.#E0");

		if(size > 0) {
			JavaRDD<Double> sortedData = data.map(duration -> duration).sortBy((Double d) -> d, true, NUM_PARTITIONS);
			JavaPairRDD<Long, Double> indexes = sortedData.zipWithIndex().mapToPair(Tuple2::swap);

			long indexMedian = (size % 2 == 0) ? ((size / 2) -1) : (((size + 1) / 2)-1);
			long indexLastOfFirstQuarter = (long) Math.ceil(size / 4.); // Calculate the number of value in one quarter
			long indexLastOfThirdQuarter = (long) Math.ceil(3 * size / 4.); // Calculate the number of value in three quarter

			median = (size % 2 == 0) ? ( indexes.lookup(indexMedian).get(0) + indexes.lookup(indexMedian + 1).get(0) ) / 2 : indexes.lookup(indexMedian).get(0);
			firstQuartile = indexes.lookup(indexLastOfFirstQuarter).get(0); // Max value of 25% of dataset
			thirdQuartile = indexes.lookup(indexLastOfThirdQuarter).get(0); // Max value of 75% of dataset
		}

		result.add("Minimum:	" + min);
		result.add("Maximum:	" + max);
		result.add("Moyenne:	" + mean);
		result.add("Mediane:	" + median);
		result.add("1er Quartile:	" + firstQuartile);
		result.add("3ème Quartile:	" + thirdQuartile);
		result.add("Histogramme:	");
		for(int i=0; i < values.length; i++) {
			result.add("Entre " + formatter.format(intervals[i]) + " et " + formatter.format(intervals[i+1]) + " :		" + values[i]);
		}
		return result;
	}

	/**
	 * Display multiple distribution ( Minimum, Maximum, Moyenne, Médiane, premier quadrants, troisième quadrants, histogramme )
	 */
	private static ArrayList<String> displayMultipleDistribution(JavaPairRDD<Integer, Double> dataSet, double[] intervals) {
		ArrayList<String> result = new ArrayList<>();
		List<Integer> keys = new ArrayList<>();
		dataSet.keys().foreach(keys::add);
		for(Integer i : keys){
			List<Double> durations = dataSet.lookup(i);
			JavaDoubleRDD data = context.parallelizeDoubles(durations);
			result.addAll(displayDistribution(data, intervals));
		}
		return result;
	}

	/**
	 * Question 1.a
	 */
	private static void getDistribOfDurationNotIDLE(JavaPairRDD<String, String> data) {
		ArrayList<String> result = new ArrayList<>();
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);

		result.add("Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1));
		result.addAll(displayDistribution(dataForStats, intervalsForDuration));
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "1a_DurationDistribution_NotIDLE");
	}

	/**
	 * Question 1.b (pattern == -1)
	 */
	private static void getDistribOfDurationIDLE(JavaPairRDD<String, String> data) {
		ArrayList<String> result = new ArrayList<>();
		JavaPairRDD<String, String> filteredData = data.filter(filterIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);

		result.add("Nombre de plages horaires correspondantes: " + filteredData.count() + " sur " + (data.count()-1));
		result.addAll(displayDistribution(dataForStats, intervalsForDuration));
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "1b_DurationDistribution_IDLE");
	}

	/**
	 * Question 1.c
	 */
	private static void getDistribOfDurationAlonePattern(JavaPairRDD<String, String> data) {
		ArrayList<String> result = new ArrayList<>();
		JavaPairRDD<String, String> filteredData = data.filter(filterAlonePattern);
		JavaPairRDD<Integer, Double> dataForStats = filteredData.mapToPair(mappingMultipleDurationForStats);
		result.addAll(displayMultipleDistribution(dataForStats, intervalsForDuration));
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "1c_DurationDistribution_AlonePattern");
	}

	/**
	 * Question 2
	 */
	private static void getDistribOfNbPatternsPerPhase(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingNbPatternsForStats);

		ArrayList<String> result = new ArrayList<>(displayDistribution(dataForStats, intervalsForNbPatterns));
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "2_NbPatternsDistributionPerPhase");
	}

	/**
	 * Question 3
	 */
	private static void getDistribOfNbJobsPerPhase(JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingNbJobsForStats);

		ArrayList<String> result = new ArrayList<>(displayDistribution(dataForStats, intervalsForNbJobs));
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "3_NbJobsDistributionPerPhase");
	}

	public interface SerializableComparator<T> extends Comparator<T>, Serializable {
		static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
			return comparator;
		}
	}

	/**
	 * Question 4.a et 4.b
	 */
	private static void getDistribOfTotalPFSAccessPerJob(JavaPairRDD<String, String> data) {
		//Question 4.a
		ArrayList<String> distrib = new ArrayList<>();
		distrib.add("--- DISTRIBUTION DU TEMPS TOTAL D'ACCES AU PFS PAR JOB ---");
		JavaPairRDD<String, String> filteredData = data.filter(filterNotIDLE);
		JavaPairRDD<Integer, Double> mapJobDuration = filteredData.flatMapToPair(p -> {
			Double duration = Double.parseDouble(p._2().split("/")[0]);
			String[] jobs = p._2().split("/")[3].split(",");
			ArrayList<Tuple2<Integer, Double>> result = new ArrayList<>();
			for(String s : jobs){
				Tuple2<Integer, Double> pair = new Tuple2<>(Integer.parseInt(s), duration);
				result.add(pair);
			}
			return result.iterator();
		});
		mapJobDuration = mapJobDuration.reduceByKey((v1, v2) -> v1 + v2);
		JavaDoubleRDD dataForStats = mapJobDuration.mapToDouble(Tuple2::_2);
		distrib.addAll(displayDistribution(dataForStats, intervalsForDuration));
		JavaRDD<String> outputDistribution = context.parallelize(distrib);
		outputDistribution.coalesce(1).saveAsTextFile(OUTPUT_URL + "4a_DistributionJobs");

		//Question 4.b
		List<Tuple2<Integer, Double>> top10 = mapJobDuration.top(10, SerializableComparator.serialize((t1, t2) -> t1._2()<t2._2() ? -1 : t1._2()>t2._2() ? 1 : 0));
		ArrayList<String> top10String = new ArrayList<>();
		top10String.add("--- TOP 10 JOBS EN TEMPS TOTAL D'ACCES AU PFS ---");
		int rang = 1;
		for(Tuple2<Integer, Double> p : top10){
			top10String.add("Numero " + Integer.toString(rang) + " : Job " + Integer.toString(p._1()) + ", duree : " + Double.toString(p._2()));
			rang++;
		}
		JavaRDD<String> outputTop10 = context.parallelize(top10String);
		outputTop10.coalesce(1).saveAsTextFile(OUTPUT_URL + "4b_DistributionJobs");
	}

	/**
	 * Question 5
	 */
	private static void getTotalDurationIDLE(JavaPairRDD<String, String> data) {
		ArrayList<String> result = new ArrayList<>();
		JavaPairRDD<String, String> filteredData = data.filter(filterIDLE);
		JavaDoubleRDD dataForStats = filteredData.mapToDouble(mappingDurationForStats);

		result.add("Resultat en microsecondes:	" + dataForStats.stats().sum());
		JavaRDD<String> output = context.parallelize(result);
		output.coalesce(1).saveAsTextFile(OUTPUT_URL + "5_getTotalDurationIDLE");
	}

	/**
	 * Question 6.a et 6.b
	 */
	private static void getPercentOfTotalTimeWhereAPatternIsAlone(JavaPairRDD<String, String> data) {
		TreeMap<Double, String> top10 = new TreeMap<>();
		JavaPairRDD<String, String> filteredNotIDLE = data.filter(filterNotIDLE);
		Double allForStats = filteredNotIDLE.mapToDouble(mappingDurationForStats).sum();

		//Question 6a
		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 22; i++) {
			patterns_selected = Integer.toString(i); //Save the pattern to allow access to it  in filterAlonePattern method
			JavaPairRDD<String, String> isPatternPresent = filteredNotIDLE.filter(filterPresentPattern);

			JavaPairRDD<String, String> filteredPatternAlone = isPatternPresent.filter(filterAloneSelectedPattern);
			JavaPairRDD<String, String> filteredPatternMultiple = isPatternPresent.filter(filterMultipleSelectedPattern);

			JavaDoubleRDD dataForStatsAlone = filteredPatternAlone.mapToDouble(mappingDurationForStats);
			JavaDoubleRDD dataForStatsMultiple = filteredPatternMultiple.mapToDouble(mappingDurationForStats);

			double sumDataAlone = dataForStatsAlone.sum();
			double sumDataMultiple = dataForStatsMultiple.sum();
			double percentageAlone = sumDataAlone / allForStats * 100;
			double percentageMultiple = sumDataMultiple / allForStats * 100;
			double representativite = percentageAlone + percentageMultiple;

			//Manage key duplication (2 patterns with the same percent)
			if(top10.containsKey(representativite)) {
				String previousPattern = top10.get(representativite);
				top10.put(representativite, previousPattern + "," + i);
			} else {
				top10.put(representativite, Integer.toString(i));
			}

			if(top10.size() > 10) {
				top10.remove(top10.firstKey());
			}
			result.add("Resultat du pattern SEUL " + i + " en millisecondes: " +	sumDataAlone + " sur " + allForStats + ". Soit " + percentageAlone + "% du temps total des phases");
			result.add("Resultat du pattern EN CONCURRENCE" + i + " en millisecondes: " +	sumDataMultiple + " sur " + allForStats + ". Soit " + percentageMultiple + "% du temps total des phases");
		}
		JavaRDD<String> output6a = context.parallelize(result);
		output6a.coalesce(1).saveAsTextFile(OUTPUT_URL + "6a_PercentOfTotalTimeForPattern");

		//Question 6b
		ArrayList<String> resultTop10 = new ArrayList<>();
		int position = 10;
		for (Map.Entry<Double, String> entry : top10.entrySet()) {
			double topPercent = entry.getKey();
			String topPattern = entry.getValue();
			String egalite = "";
			if(topPattern.split(",").length > 1) {
				egalite = "[EGALITE]";
			}
			resultTop10.add(position + egalite + " : Pattern " + topPattern+ " avec " + topPercent + "% du temps total.");
			position--;
		}
		JavaRDD<String> output6b = context.parallelize(resultTop10);
		output6b.coalesce(1).saveAsTextFile(OUTPUT_URL + "6b_top10PercentOfTotalTime");
	}

	/**
	 * Question 7
	 */
	private static void getLinesMatchingWithPatterns(JavaPairRDD<String, String> data) {
		//TODO: temps linéaire
		JavaPairRDD<String, String> filteredData = data.filter(filterMatchingPatterns);
		filteredData.coalesce(1).saveAsTextFile(OUTPUT_URL + "7_getLinesMatchingWithPatterns_" + String.join("_", target_patterns));
	}

	/**
	 * Main program
	 * @param args program's arguments
	 */
	public static void main(String[] args) {
		if (args.length > 0 && Arrays.asList(argsPossibilities).contains(args[0])) {
			SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
			context = new JavaSparkContext(conf);
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
				case "2":
					getDistribOfNbPatternsPerPhase(data);
					break;
				case "3":
					getDistribOfNbJobsPerPhase(data);
					break;
				case "4":
					getDistribOfTotalPFSAccessPerJob(data);
					break;
				case "5":
					getTotalDurationIDLE(data);
					break;
				case "6":
					getPercentOfTotalTimeWhereAPatternIsAlone(data);
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
