package tp3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;

public class Exercice2 {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: Exercice2 <csv_file>");
            System.exit(1);
        }

        String csvPath = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("TP3-Exercice2-Etudiants")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvPath);

        System.out.println("==========================================");
        System.out.println("1. Nombre d'etudiants par groupe :");
        df.groupBy("groupe").count().show();

        System.out.println("2. Nombre d'etudiants par specialite :");
        df.groupBy("specialite").count().show();

        System.out.println("3. Moyenne generale par niveau et specialite :");
        df.groupBy("niveau", "specialite")
          .agg(avg("moyenne").alias("moyenne_generale"))
          .show();
        System.out.println("==========================================");

        spark.stop();
    }
}

