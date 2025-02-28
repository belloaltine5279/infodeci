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
    val businessFile = "/home1/ma618349/M2/logic/travail/donnee/yelp_academic_dataset_business.json"
    //val businessCheckinFile = "/home/bello/logic/travail/donnee/yelp_academic_dataset_checkin.json"
    val businessCheckinFile = "/home1/ma618349/M2/logic/travail/donnee/yelp_academic_dataset_checkin.json"
    val csvBusinessDir = "/home1/ma618349/M2/logic/travail/donnee/yelp_academic_dataset_tip.csv"
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


val dateFormat = "yyyy-MM-dd" 

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



  val categories = business.select("categories")
  .withColumn("categories", col("categories").cast("string"))
  .withColumn("categories", expr("substring(categories, 1, 255)")) // Limiter la taille de la colonne


  val mots = categories
      .withColumn("mot", explode(split(col("categories"), ",")))
      .withColumn("mot", trim(lower(col("mot"))))

    val occurences = mots
      .groupBy("mot")
      .count()

// 1. Créer une nouvelle colonne 'category_list' pour chaque catégorie dans les 'categories'
val businessWithCategoryList = businessTable
  .withColumn("category_list", explode(split(col("categories"), ",")))
  .withColumn("category_list", trim(lower(col("category_list")))) // Nettoyer les catégories (espaces, minuscules)

// 2. Créer un DataFrame avec les catégories populaires ayant plus de 10000 occurrences
val motsRepetitifs = occurences.filter($"count" >= 10000).withColumnRenamed("mot", "mot_populaire").as("motsRepetitifs")

// 3. Créer un DataFrame avec les catégories moins populaires ayant entre 0 et 9999 occurrences
val motsMoinsPopulaires = occurences.filter($"count" < 10000).withColumnRenamed("mot", "mot_moins_populaire").as("motsMoinsPopulaires")

// 4. Joindre businessTable avec motsRepetitifs et motsMoinsPopulaires pour associer les catégories populaires et moins populaires aux business
val businessWithType = businessWithCategoryList
  .join(motsRepetitifs, businessWithCategoryList("category_list") === motsRepetitifs("mot_populaire"), "left")
  .join(motsMoinsPopulaires, businessWithCategoryList("category_list") === motsMoinsPopulaires("mot_moins_populaire"), "left")
  .withColumn("type", when($"motsRepetitifs.count".isNotNull, $"motsRepetitifs.mot_populaire") // Si catégorie populaire
    .otherwise(when($"motsMoinsPopulaires.count".isNotNull, $"motsMoinsPopulaires.mot_moins_populaire") // Si catégorie moins populaire
    .otherwise("unknown"))) // Si aucune catégorie n'est trouvée

// 5. Trouver la catégorie la plus fréquente pour chaque business en prenant la catégorie la plus populaire ou la moins populaire
val businessTypeFinal = businessWithType
  .groupBy("business_id")
  .agg(
    first("type").alias("type") // On prend la première catégorie trouvée (la plus populaire si elle existe, sinon la deuxième)
  )
  
  val businessTableWithType = businessTable
  .join(businessTypeFinal, Seq("business_id"), "left")  // Effectuer une jointure gauche sur business_id

// 2. Ajouter les autres colonnes nécessaires
val businessWithAllColumns = businessTableWithType
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
    "review_count",
    "type"  // on ajoute la colonne `type`
  )

  businessWithAllColumns.show(100, truncate= false)


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

// faits.write.mode("overwrite").jdbc(oracleUrl, "faits", oracleProperties)
// businessTable1000.write.mode("overwrite").jdbc(oracleUrl, "business", oracleProperties)
//businessWithAllColumns.write.mode("overwrite").jdbc(oracleUrl, "business", oracleProperties)
// userstable.write.mode("overwrite").jdbc(oracleUrl, "users", oracleProperties)
// datesTable1000.write.mode("overwrite")jdbc(oracleUrl, "dates", oracleProperties)
// locationTable.write.mode("overwrite").jdbc(oracleUrl, "location", oracleProperties)
// friendTable.write.mode("overwrite").jdbc(oracleUrl, "friend", oracleProperties)
// eliteTable.write.mode("overwrite").jdbc(oracleUrl, "elite", oracleProperties)

    spark.stop()
  }
}
