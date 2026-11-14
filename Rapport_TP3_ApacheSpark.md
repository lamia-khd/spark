# Rapport de Travaux Pratiques : TP 3
## Installation, Configuration d'Apache Spark et Applications Distribuées en Java

---

## 1. Introduction et Architecture du Cluster

L'objectif de ce TP est de déployer et de configurer un cluster **Apache Spark 3.2.1** en mode *Standalone* sur une architecture multi-nœuds interconnectée au système de fichiers distribué **Hadoop HDFS 3.3.6**. Deux applications Java complètes sont développées :
1. Une application exploitant les **RDDs (Resilient Distributed Datasets)** pour le traitement textuel distribué.
2. Une application exploitant **Spark SQL (Datasets / DataFrames)** pour le traitement de données tabulaires structurées (CSV).

### Topologie du Cluster :
- **M1 (`hadoop-master`) :** Nœud Maître (Spark Master, Hadoop NameNode, YARN ResourceManager).
- **M2 (`hadoop-worker1`) :** Nœud Travailleur (Spark Worker, Hadoop DataNode, YARN NodeManager).
- **M3 (`hadoop-worker2`) :** Nœud Travailleur (Spark Worker, Hadoop DataNode, YARN NodeManager).

---

## 2. Guide d'Installation et Configuration Pas à Pas

### Étape 1 : Téléchargement et Déploiement de Spark (M1, M2, M3)
L'archive officielle `spark-3.2.1-bin-hadoop3.2.tgz` est extraite dans le répertoire standard `/opt/spark` sur les trois nœuds :
```bash
tar -xzf spark-3.2.1-bin-hadoop3.2.tgz -C /opt/
mv /opt/spark-3.2.1-bin-hadoop3.2 /opt/spark
```

### Étape 2 : Variables d'Environnement (`~/.bashrc`)
Sur chaque nœud, les variables suivantes ont été ajoutées dans `~/.bashrc` :
```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
```

### Étape 3 : Configuration du Cluster (`conf/spark-defaults.conf`)
Sur **M1 (`hadoop-master`)**, le fichier `/opt/spark/conf/spark-defaults.conf` a été configuré comme suit :
```properties
spark.master                     spark://hadoop-master:7077
spark.submit.deployMode          client
spark.driver.memory              512m
spark.executor.memory            512m
spark.executor.cores             1
spark.hadoop.fs.defaultFS        hdfs://hadoop-master:8020
spark.driver.host                hadoop-master
spark.sql.shuffle.partitions     2
```

Les nœuds esclaves sont déclarés dans `/opt/spark/conf/workers` :
```text
hadoop-worker1
hadoop-worker2
```

Et dans `/opt/spark/conf/spark-env.sh` :
```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1g
```

### Étape 4 : Démarrage des Services et Vérification
- **Sur M1 :**
  ```bash
  $SPARK_HOME/sbin/start-master.sh
  ```
- **Sur M2 et M3 :**
  ```bash
  $SPARK_HOME/sbin/start-worker.sh spark://hadoop-master:7077
  ```

#### Vérification des démons avec `jps` :
- **Sur `hadoop-master` :** `Master`, `NameNode`, `SecondaryNameNode`, `ResourceManager`.
- **Sur `hadoop-worker1` & `hadoop-worker2` :** `Worker`, `DataNode`, `NodeManager`.

#### Tableau de bord Web :
L'interface `http://localhost:8080` confirme l'état **ALIVE** avec **2 Alive Workers**.

---

## 3. Exercice 1 : Fréquence et Longueur des Mots (Spark Core / RDD)

### Énoncé :
1. Écrire un programme Spark en Java pour compter la fréquence des mots dans un fichier texte.
2. Écrire un programme Spark en Java pour compter le nombre de mots de chaque longueur présente dans un fichier texte.

### Code Source Java (`tp3/Exercice1.java`) :
```java
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

        // Lecture du fichier texte depuis HDFS
        JavaRDD<String> lines = sc.textFile(inputFile);

        // Découpage en mots propres
        JavaRDD<String> words = lines
            .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase().trim())
            .filter(word -> !word.isEmpty());

        // 1. Fréquence d'apparition de chaque mot
        JavaPairRDD<String, Integer> wordCounts = words
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey(Integer::sum);

        wordCounts.saveAsTextFile(outputDir + "/word_frequency");

        // 2. Nombre de mots de chaque longueur
        JavaPairRDD<Integer, Integer> lengthCounts = words
            .mapToPair(word -> new Tuple2<>(word.length(), 1))
            .reduceByKey(Integer::sum);

        lengthCounts.saveAsTextFile(outputDir + "/word_length_count");

        sc.stop();
    }
}
```

### Compilation et Exécution :
```bash
# Compilation et empaquetage
CP=$(find /opt/spark/jars/ -name "*.jar" | paste -sd ":" -)
javac -encoding UTF-8 -cp "$CP" -d tp3/classes tp3/Exercice1.java tp3/Exercice2.java
jar -cvf tp3.jar -C tp3/classes/ .

# Soumission du job Spark
spark-submit \
  --class tp3.Exercice1 \
  --master spark://hadoop-master:7077 \
  tp3.jar \
  hdfs://hadoop-master:8020/tp3/input/texte.txt \
  hdfs://hadoop-master:8020/tp3/output
```

### Réponses et Résultats Obtenus :

#### 1. Fréquence d'apparition des mots (`/tp3/output/word_frequency/*`) :
| Mot | Fréquence | Mot | Fréquence | Mot | Fréquence |
| :--- | :---: | :--- | :---: | :--- | :---: |
| `spark` | **3** | `scale` | **1** | `java` | **1** |
| `apache` | **2** | `unified` | **1** | `scala` | **1** |
| `is` | **2** | `engine` | **1** | `python` | **1** |
| `and` | **2** | `data` | **1** | `apis` | **1** |
| `analytics` | **1** | `processing` | **1** | `general` | **1** |

#### 2. Nombre de mots par longueur (`/tp3/output/word_length_count/*`) :
| Longueur du mot (caractères) | Nombre d'occurrences |
| :---: | :---: |
| **1** | 2 |
| **2** | 3 |
| **3** | 3 |
| **4** | 5 |
| **5** | 7 |
| **6** | 4 |
| **7** | 3 |
| **8** | 1 |
| **9** | 1 |
| **10** | 1 |

---

## 4. Exercice 2 : Traitement Structuré avec Spark SQL

### Données d'Entrée (`hdfs://hadoop-master:8020/tp3/input/etudiants.csv`) :
```csv
NCE,nom,prenom,specialite,niveau,groupe,moyenne
00025478,Ben Saber,Ahmed,DSI,3,2,12.5
00025479,Trabelsi,Sarra,DSI,3,2,14.0
00025480,Gharbi,Ali,SEM,2,1,10.0
00025481,Ayadi,Mariem,DSI,3,1,15.5
00025482,Mansour,Khaled,SEM,2,1,11.0
00025483,Mejri,Fatma,SEM,3,2,13.5
```

### Code Source Java (`tp3/Exercice2.java`) :
```java
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

        // Lecture du CSV avec inférence du schéma
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvPath);

        System.out.println("==========================================");
        // 1. Nombre d'étudiants par groupe
        System.out.println("1. Nombre d'etudiants par groupe :");
        df.groupBy("groupe").count().show();

        // 2. Nombre d'étudiants par spécialité
        System.out.println("2. Nombre d'etudiants par specialite :");
        df.groupBy("specialite").count().show();

        // 3. Moyenne générale par niveau et spécialité
        System.out.println("3. Moyenne generale par niveau et specialite :");
        df.groupBy("niveau", "specialite")
          .agg(avg("moyenne").alias("moyenne_generale"))
          .show();
        System.out.println("==========================================");

        spark.stop();
    }
}
```

### Exécution du Job :
```bash
spark-submit \
  --class tp3.Exercice2 \
  --master spark://hadoop-master:7077 \
  tp3.jar \
  hdfs://hadoop-master:8020/tp3/input/etudiants.csv
```

### Réponses aux Questions de l'Exercice 2 :

#### Question 1 : Le nombre d'étudiants par groupe
```text
+------+-----+
|groupe|count|
+------+-----+
|     1|    3|
|     2|    3|
+------+-----+
```

#### Question 2 : Le nombre d'étudiants par spécialité
```text
+----------+-----+
|specialite|count|
+----------+-----+
|       DSI|    3|
|       SEM|    3|
+----------+-----+
```

#### Question 3 : Calcul de la moyenne générale par niveau et spécialité
```text
+------+----------+----------------+
|niveau|specialite|moyenne_generale|
+------+----------+----------------+
|     3|       DSI|            14.0|
|     2|       SEM|            10.5|
|     3|       SEM|            13.5|
+------+----------+----------------+
```

---

## 5. Conclusion

Ce TP a permis d'acquérir une maîtrise pratique complète d'un environnement Big Data combinant **Apache Spark** et **Hadoop HDFS** :
- Utilisation des **RDDs** avec `mapToPair` et `reduceByKey` pour les traitements personnalisés sur des données non structurées.
- Utilisation de **Spark SQL** avec l'optimiseur **Catalyst** pour des requêtes relationnelles hautement performantes (`groupBy`, `count`, `agg(avg(...))`).

