package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class MainPLE {

	private static final String PHASES_FILE_URL = "/raw_data/ALCF_repo/phases.csv";
	private static final String JOBS_FILE_URL = "/raw_data/ALCF_repo/jobs.csv";
	private static final String PATTERNS_FILE_URL = "/raw_data/ALCF_repo/patterns.csv";

	private void getDistribOfDurationNotIDLE() {} // Question 1.a
	private void getDistribOfDurationIDLE() {} // Question 1.b (pattern = -1)
	private void getDistribOfDurationAlonePattern(String pattern) {} // Question 1.c (foreach patterns - 22)

	private void getDistribOfNbPatternsPerPhase() {} // Question 2.a
	private void getDistribOfNbJobsPerPhase() {} // Question 3.a

	private void getDistribOfTotalPFSAccessPerJob() {} // Question 4.a
	private void getTop10JobsTotalPFSAccess() {} // Question 4.b

	private void getTotalDurationIDLE() {} // Question 5

	private void getDistribOfTotalTimeWithAPatternAlone() {} // Question 6.a
	private void getTop10patterns() {} // Question 6.b

	private void getLinesMatchingWithPatterns(String[] patterns) {} // Question 7


	private static void startProgram(String[] target_patterns) {
		SparkConf conf = new SparkConf().setAppName("Projet PLE 2019");
		JavaSparkContext context = new JavaSparkContext(conf);


		JavaRDD<String[]> rdd = context.textFile(PHASES_FILE_URL).map(line -> line.split(";"));
		rdd= rdd.coalesce(5, true); // set number of partitions

		/*** QUESION 7 ***/
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

	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("Expected exactly 4 arguments -- " + args.length + " given.");
		} else {
			startProgram(args);
		}
	}

}
