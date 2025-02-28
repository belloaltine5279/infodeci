error id: groupBy.
file://<WORKSPACE>/src/main/scala/SimpleApp.scala
empty definition using pc, found symbol in pc: groupBy.
empty definition using semanticdb
|empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/types/mots/groupBy.
	 -org/apache/spark/sql/types/mots/groupBy#
	 -org/apache/spark/sql/types/mots/groupBy().
	 -org/apache/spark/sql/functions/mots/groupBy.
	 -org/apache/spark/sql/functions/mots/groupBy#
	 -org/apache/spark/sql/functions/mots/groupBy().
	 -spark/implicits/mots/groupBy.
	 -spark/implicits/mots/groupBy#
	 -spark/implicits/mots/groupBy().
	 -mots/groupBy.
	 -mots/groupBy#
	 -mots/groupBy().
	 -scala/Predef.mots.groupBy.
	 -scala/Predef.mots.groupBy#
	 -scala/Predef.mots.groupBy().

Document text:

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties


object SimpleApp {
  def main(args: Array[String]): Unit = {
    // Initialisation de Spark
    val spark = SparkSession.builder
      .appName("ETL")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.autoBroadcastJoinThreshold", "50MB") // Correction
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.storageFraction", "0.2")
      .config("spark.network.timeout", "300s")
      .config("spark.executor.heartbeatInterval", "60s")   
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    // Chemins des fichiers
    val businessFile = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_business.json"
    //val businessCheckinFile = "/home/bello/logic/travail/donnee/yelp_academic_dataset_checkin.json"
    val businessCheckinFile = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_checkin.json"
    val csvBusinessDir = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_tip.csv"
    //val csvBusinessDir = "/home/bello/logic/travail/donnee/yelp_academic_dataset_tip.csv"
    // Chargement des données
    val business = spark.read.json(businessFile)
      // .option("fetchsize", "10000") // Adjust fetch size for JDBC reads
      // .option("partitionColumn", "id")
      // .option("lowerBound", "1")
      // .option("upperBound", "1000000")
      // .option("numPartitions", "10")
      .cache()
    val businessCheckin = spark.read.json(businessCheckinFile).cache()
    val businessCSV = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvBusinessDir)
      .select("business_id", "date", "user_id").cache()
    // println("business schema :")
    //   business.printSchema()
    // println("businessCheckin schema :")
    //   businessCheckin.printSchema()
    // println("businessCSV schema :")
    //   businessCSV.printSchema()


    val url = "jdbc:postgresql://stendhal:5432/tpid2020"
    val properties = new Properties()
    properties.setProperty("user", "tpid")
    properties.setProperty("password", "tpid")
    properties.setProperty("driver", "org.postgresql.Driver") // Assure-toi d'avoir le pilote PostgreSQL dans ton classpath

    // Liste des tables à interroger
    //val tables = Seq("yelp.user", "yelp.review", "yelp.friend", "yelp.elite")

    // Requête SQL pour obtenir les schémas des tables
    /*
    tables.foreach { table =>
      val query = s"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema || '.' || table_name = 'table'
        ORDER BY ordinal_position
      """

      // Charger les résultats dans un DataFrame
      val schemaDF = spark.read.jdbc(url, s"(query) AS schema_query", properties)

      // Afficher le schéma de la table
      println(s"Schéma de la table table :")
      schemaDF.show(truncate = false)
    }

*/ 


    // Charger les données sources
//val business = spark.read.json("chemin/vers/business.json")
//val businessCheckin = spark.read.json("chemin/vers/businessCheckin.json")
//val businessCSV = spark.read.option("header", "true").csv("chemin/vers/businessCSV.csv")
val users = spark.read.jdbc(url, "yelp.user", properties).cache()
val reviews = spark.read.jdbc(url, "yelp.review", properties).cache()
val friends = spark.read.jdbc(url, "yelp.friend", properties).cache()
val elite = spark.read.jdbc(url, "yelp.elite", properties).cache()

// Table de faits
val faits = reviews.select(
  "business_id",
  "user_id",
  "review_id",
  "date",
  "stars",
  "useful",
  "funny",
  "cool"
)
.withColumn("business_id", col("business_id").cast("string"))
.withColumn("user_id", col("user_id").cast("string"))
.withColumn("review_id", col("review_id").cast("string"))
.withColumn("date", col("date").cast("date"))
.withColumn("stars", col("stars").cast("double"))
.withColumn("useful", col("useful").cast("int"))
.withColumn("funny", col("funny").cast("int"))
.withColumn("cool", col("cool").cast("int"))
.cache()

// faits.show()

// Table business
val businessTable = business
  .select(
    "business_id",
    "name",
    "city",
    "state",
    "postal_code",
    "latitude",
    "longitude",
    "categories",
    "stars",
    "review_count"
  )
  .withColumn("business_id", col("business_id").cast("string"))
  .withColumn("name", col("name").cast("string"))
  .withColumn("city", col("city").cast("string"))
  .withColumn("state", col("state").cast("string"))
  .withColumn("postal_code", col("postal_code").cast("string"))
  .withColumn("latitude", col("latitude").cast("double"))
  .withColumn("longitude", col("longitude").cast("double"))
  .withColumn("categories", col("categories").cast("string"))
  .withColumn("categories", expr("substring(categories, 1, 255)")) // Limiter la taille de la colonne
  .withColumn("stars", col("stars").cast("double"))
  //.withColumn("type", lit("type").cast("string"))

  val businessTable1000 = businessTable

// businessTable1000.show()


// Table users
// Si nécessaire, définir un format spécifique pour la date
val dateFormat = "yyyy-MM-dd"  // Adaptez ce format si nécessaire

val usersTable = users
  .select(
    "user_id",
    "name",
    "review_count",
    "average_stars",
    "yelping_since",
    "fans"
  )
  .withColumn("user_id", col("user_id").cast("string"))
  .withColumn("name", col("name").cast("string"))
  .withColumn("review_count", col("review_count").cast("int"))
  .withColumn("average_stars", col("average_stars").cast("double"))
  // Utilisation de to_date pour gérer des formats de date spécifiques
  .withColumn("yelping_since", to_date(col("yelping_since"), dateFormat))
  .withColumn("fans", col("fans").cast("int"))

/*

  val categories = business.select("categories")
  .withColumn("categories", col("categories").cast("string"))
  .withColumn("categories", expr("substring(categories, 1, 255)")) // Limiter la taille de la colonne


  val mots = categories
      .withColumn("mot", explode(split(col("categories"), ",")))
      .withColumn("mot", trim(lower(col("mot"))))

    val occurences = mots
      .groupBy("mot")
      .count()

    val motsRepetitifs = occurences.filter($"count" >= 10000)
    motsRepetitifs.show(10000, truncate = false)


    val businessWithType = businessTable
  .join(motsRepetitifs, 
        businessTable("categories").contains(motsRepetitifs("mot")), 
        "left")
  .withColumn("type", when($"count".isNotNull, $"mot"))
// Afficher le résultat pour vérifier la nouvelle colonne "type"
businessWithType.show(100, false)

    //businessTable1000 = businessTable1000

    //businessTable1000.show(10000, truncate = false)

 */

 // 1. Créer une nouvelle colonne 'category_list' pour chaque catégorie dans les 'categories'
val businessWithCategoryList = businessTable
  .withColumn("category_list", explode(split(col("categories"), ",")))
  .withColumn("category_list", trim(lower(col("category_list")))) // Nettoyer les catégories (espaces, minuscules)

// 2. Créer un DataFrame avec les catégories populaires ayant plus de 10000 occurrences
val motsRepetitifs = occurences.filter($"count" >= 10000)

// 3. Joindre businessTable avec motsRepetitifs pour associer les catégories populaires aux business
val businessWithType = businessWithCategoryList
  .join(motsRepetitifs, businessWithCategoryList("category_list") === motsRepetitifs("mot"), "left")
  .withColumn("type", when($"count".isNotNull, $"mot").otherwise("unknown")) // Attribuer le type basé sur les catégories populaires

// 4. Trouver la catégorie la plus fréquente pour chaque business (celle qui correspond au plus grand nombre de catégories populaires)
val businessTypeFinal = businessWithType
  .groupBy("business_id")
  .agg(
    first("type").alias("type") // On prend simplement la première catégorie populaire associée au business
  )

// 5. Afficher le résultat
businessTypeFinal.show(1000, truncate = false)

// val userstable1000 = usersTable

// userstable1000.show()


// Table dates

//val datesTable = reviews
// .select(
//    date_format(col("date"), "yyyyMMdd").cast("int").as("date_id"),
//    col("date"),
//    year(col("date")).as("year"),
//    month(col("date")).as("month"),
//    dayofmonth(col("date")).as("day")
//  )
//  .withColumn("date_id", monotonically_increasing_id().cast("int"))
//  .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
//  .withColumn("year", year(col("date")))
//  .withColumn("month", month(col("date")))
//  .withColumn("day", dayofmonth(col("date")))
//  .distinct()

  val datesTable = reviews
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .select("date_id", "date", "year", "month", "day")
    .distinct()

    val datesTable1000 = datesTable

// Table location
val locationTable = business.select(
    monotonically_increasing_id().cast("int").as("location_id"),
    col("city"),
    col("state"),
    col("postal_code")
)
.withColumn("city", col("city").cast("string"))
.withColumn("state", col("state").cast("string"))
.withColumn("postal_code", col("postal_code").cast("string"))



// Table friend
val friendTable = friends
  .select(
    "user_id",
    "friend_id"
  )
  .withColumn("user_id", col("user_id").cast("string"))
  .withColumn("friend_id", col("friend_id").cast("string"))

// Table elite
val eliteTable = elite
  .select(
    "user_id",
    "year"
  )

    // Connexion à Oracle
val oracleUrl = "jdbc:oracle:thin:@stendhal:1521/enss2024" // Remplacez par votre URL Oracle
val oracleProperties = new java.util.Properties()
oracleProperties.setProperty("user", "ma618349") // Remplacez par votre utilisateur Oracle
oracleProperties.setProperty("password", "ma618349") // Remplacez par votre mot de passe Oracle
oracleProperties.setProperty("driver", "oracle.jdbc.OracleDriver")
// oracleProperties.setProperty("batchsize", "500")

// Écrire les tables dans Oracle
//faits.write.mode("overwrite").jdbc(oracleUrl, "faits", oracleProperties)
// businessTable1000.write.mode("overwrite").jdbc(oracleUrl, "business", oracleProperties)
// userstable1000.write.mode("overwrite").jdbc(oracleUrl, "users", oracleProperties)
// datesTable1000.write.mode("overwrite")jdbc(oracleUrl, "dates", oracleProperties)
// locationTable.write.mode("overwrite").jdbc(oracleUrl, "location", oracleProperties)
// friendTable.write.mode("overwrite").jdbc(oracleUrl, "friend", oracleProperties)
// eliteTable.write.mode("overwrite").jdbc(oracleUrl, "elite", oracleProperties)



/*
    // Transformation des données pour correspondre aux tables Oracle
    val faits = business
      .select(
        col("business_id").as("business_id"),
        col("review_id").as("review_id"),
        col("city").as("city"),
        col("stars").as("stars"),
        col("categories").as("categories")
      ).withColumn(
  "review_id",
  coalesce(col("review_id"), lit("N/A")) // Remplace les valeurs manquantes par "N/A"
)
    val users = businessCSV
      .select(
        col("user_id").cast(StringType).as("user_id"),
        col("date").cast(DateType).as("yelping_since")
      )
      .withColumn("average_stars", lit(null).cast(DoubleType)) // Colonne manquante
      .withColumn("name", lit(null).cast(StringType)) // Colonne manquante
      .withColumn("fans", lit(null).cast(IntegerType)) // Colonne manquante
      .withColumn("compliment_cool", lit(null).cast(IntegerType)) // Colonne manquante
      .withColumn("review_count", lit(null).cast(IntegerType)) // Colonne manquante

    val dates = businessCheckin
      .select(
        monotonically_increasing_id().cast(IntegerType).as("date_id"), // Génération d'un ID unique
        col("date").cast(DateType).as("dates")
      )

    val location = business
      .select(
        col("city").as("city"),
        col("state").as("state"),
        col("postal_code").as("postal_code"),
        col("latitude").as("latitude"),
        col("longitude").as("longitude")
      )
      .withColumn("location_id", monotonically_increasing_id().cast(IntegerType)) // Génération d'un ID unique

    // Connexion à Oracle
    val oracleUrl = "jdbc:oracle:thin:@stendhal:1521/enss2024" // Remplacez par votre URL Oracle
    val oracleProperties = new java.util.Properties()
    oracleProperties.setProperty("user", "ma618349") // Remplacez par votre utilisateur Oracle
    oracleProperties.setProperty("password", "ma618349") // Remplacez par votre mot de passe Oracle
    oracleProperties.setProperty("driver", "oracle.jdbc.OracleDriver")

    // Écriture des données dans les tables Oracle
    faits.write
      .mode(SaveMode.Append)
      .jdbc(oracleUrl, "faits", oracleProperties)

    users.write
      .mode(SaveMode.Append)
      .jdbc(oracleUrl, "users", oracleProperties)

    dates.write
      .mode(SaveMode.Append)
      .jdbc(oracleUrl, "dates", oracleProperties)

    location.write
      .mode(SaveMode.Append)
      .jdbc(oracleUrl, "location", oracleProperties)

    // Arrêt de la session Spark
    */
    spark.stop()
  }
}

```

#### Short summary: 

empty definition using pc, found symbol in pc: groupBy.