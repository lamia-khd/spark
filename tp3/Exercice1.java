package tp3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Exercice1 {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Exercice1 <input_file> <output_dir>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputDir = args[1];

        SparkConf conf = new SparkConf().setAppName("TP3-Exercice1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<String> words = lines
            .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase().trim())
            .filter(word -> !word.isEmpty());

        // 1. Fréquence d'apparition de chaque mot
        JavaPairRDD<String, Integer> wordCounts = words
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey(Integer::sum);

        wordCounts.saveAsTextFile(outputDir + "/word_frequency");

        // 2. Nombre de mots par longueur
        JavaPairRDD<Integer, Integer> lengthCounts = words
            .mapToPair(word -> new Tuple2<>(word.length(), 1))
            .reduceByKey(Integer::sum);

        lengthCounts.saveAsTextFile(outputDir + "/word_length_count");

        sc.stop();
    }
}
