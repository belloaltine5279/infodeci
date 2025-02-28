file://<WORKSPACE>/src/main/scala/SimpleApp.scala
### java.lang.IndexOutOfBoundsException: -1

occurred in the presentation compiler.

presentation compiler configuration:


action parameters:
offset: 6399
uri: file://<WORKSPACE>/src/main/scala/SimpleApp.scala
text:
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



  val categories = business.select("categories")
  .withColumn("categories", col("categories").cast("string"))
  .withColumn("categories", expr("substring(categories, 1, 255)")) // Limiter la taille de la colonne


  val mots = categories
      .withColumn("mot", explode(split(col("categories"), ",")))
      .withColumn("mot", trim(lower(col("mot"))))

    val occurences = mots
      .groupBy("mot")
      .count()

val occurences = mots(@@)
/*
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
/*
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
*/
/*
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
    "type"  // Ajoute la colonne `type`
  )

  businessWithAllColumns.show(100, truncate= false
  )
  */
  /*
val businessWithType = businessWithCategoryList
  .join(motsRepetitifs, businessWithCategoryList("category_list") === motsRepetitifs("mot_populaire"), "left")
  .join(motsMoinsPopulaires, businessWithCategoryList("category_list") === motsMoinsPopulaires("mot_moins_populaire"), "left")
  .withColumn("business_type", when($"motsRepetitifs.count".isNotNull, $"motsRepetitifs.mot_populaire") // Si catégorie populaire
    .otherwise(when($"motsMoinsPopulaires.count".isNotNull, $"motsMoinsPopulaires.mot_moins_populaire") // Si catégorie moins populaire
    .otherwise("unknown"))) // Si aucune catégorie n'est trouvée

// 5. Trouver la catégorie la plus fréquente pour chaque business en prenant la catégorie la plus populaire ou la moins populaire
val businessTypeFinal = businessWithType
  .groupBy("business_id")
  .agg(
    first("business_type").alias("type") // Changer ici pour utiliser business_type
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
    "type"  // Ajoute la colonne `type` ici après avoir évité le conflit
  )
  */
  /*
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
  .withColumn("category_type", 
    when($"motsRepetitifs.count".isNotNull, $"motsRepetitifs.mot_populaire") // Si catégorie populaire
    .otherwise(when($"motsMoinsPopulaires.count".isNotNull, $"motsMoinsPopulaires.mot_moins_populaire") // Si catégorie moins populaire
    .otherwise("unknown"))) // Si aucune catégorie n'est trouvée

// 5. Trouver la catégorie la plus fréquente parmi toutes celles présentes dans les business (populaires et moins populaires)
val businessWithMaxType = businessWithType
  .groupBy("business_id")
  .agg(
    // Compter les occurrences de chaque catégorie
    collect_list("category_type").alias("all_categories"),
    collect_list("motsRepetitifs.count").alias("popular_count"),
    collect_list("motsMoinsPopulaires.count").alias("less_popular_count")
  )
  .withColumn("max_count", 
    greatest(
      coalesce($"popular_count", lit(0)),
      coalesce($"less_popular_count", lit(0))
    )) // On prend la catégorie ayant le plus grand nombre d'occurrences

// 6. Pour chaque business, attribuer la catégorie avec le max de count
val businessTypeFinal = businessWithMaxType
  .withColumn("type", 
    when($"max_count" === $"popular_count", $"motsRepetitifs.mot_populaire")
    .otherwise($"motsMoinsPopulaires.mot_moins_populaire")) // Assigner la catégorie ayant le max de count

// 7. Joindre le DataFrame final avec les autres informations de business
val businessTableWithType = businessTable
  .join(businessTypeFinal, Seq("business_id"), "left")  // Effectuer une jointure gauche sur business_id

// 8. Ajouter les autres colonnes nécessaires
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
    "type"  // Ajoute la colonne `type`
  )

businessWithAllColumns.show(100, truncate = false)
*/
/*
// Étape 1: Calculer la catégorie la plus fréquente (populaire ou moins populaire)
val businessWithCategoryList = businessTable
  .withColumn("category_list", explode(split(col("categories"), ",")))
  .withColumn("category_list", trim(lower(col("category_list")))) // Nettoyer les catégories (espaces, minuscules)

// Étape 2: Créer un DataFrame avec les catégories populaires ayant plus de 10000 occurrences
val motsRepetitifs = occurences.filter($"count" >= 10000).withColumnRenamed("mot", "mot_populaire").as("motsRepetitifs")

// Étape 3: Créer un DataFrame avec les catégories moins populaires ayant entre 0 et 9999 occurrences
val motsMoinsPopulaires = occurences.filter($"count" < 10000).withColumnRenamed("mot", "mot_moins_populaire").as("motsMoinsPopulaires")

// Étape 4: Joindre businessTable avec motsRepetitifs et motsMoinsPopulaires pour associer les catégories populaires et moins populaires aux business
val businessWithType = businessWithCategoryList
  .join(motsRepetitifs, businessWithCategoryList("category_list") === motsRepetitifs("mot_populaire"), "left")
  .join(motsMoinsPopulaires, businessWithCategoryList("category_list") === motsMoinsPopulaires("mot_moins_populaire"), "left")
  .withColumn("category_type", 
    when($"motsRepetitifs.count".isNotNull, $"motsRepetitifs.mot_populaire") // Si catégorie populaire
    .otherwise(when($"motsMoinsPopulaires.count".isNotNull, $"motsMoinsPopulaires.mot_moins_populaire") // Si catégorie moins populaire
    .otherwise("unknown"))) // Si aucune catégorie n'est trouvée

// Étape 5: Aplatir les listes pour comparer les occurrences (maximum) de chaque catégorie
val businessWithMaxType = businessWithType
  .groupBy("business_id")
  .agg(
    // Aplatir les listes pour récupérer les catégories
    collect_list("category_type").alias("all_categories"),
    collect_list("motsRepetitifs.count").alias("popular_count"),
    collect_list("motsMoinsPopulaires.count").alias("less_popular_count")
  )
  .withColumn("max_popular_count", 
    array_max($"popular_count")) // Extraire la valeur maximale dans la liste des catégories populaires
  .withColumn("max_less_popular_count", 
    array_max($"less_popular_count")) // Extraire la valeur maximale dans la liste des catégories moins populaires

// Étape 6: Trouver la catégorie ayant le nombre d'occurrences maximum parmi les populaires et les moins populaires
val businessTypeFinal = businessWithMaxType
  .withColumn("type", 
    when($"max_popular_count" >= $"max_less_popular_count", $"motsRepetitifs.mot_populaire")
    .otherwise($"motsMoinsPopulaires.mot_moins_populaire")) // Assigner la catégorie ayant le max de count

// Étape 7: Joindre le DataFrame final avec les autres informations de business
val businessTableWithType = businessTable
  .join(businessTypeFinal, Seq("business_id"), "left")  // Effectuer une jointure gauche sur business_id

// Étape 8: Ajouter les autres colonnes nécessaires
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
    "type"  // Ajoute la colonne `type`
  )
  
businessWithAllColumns.show(100, truncate = false)
*/

// Étape 1: Calculer la catégorie la plus fréquente (populaire ou moins populaire)
val businessWithCategoryList = businessTable
  .withColumn("category_list", explode(split(col("categories"), ",")))
  .withColumn("category_list", trim(lower(col("category_list")))) // Nettoyer les catégories (espaces, minuscules)

// Étape 2: Créer un DataFrame avec les catégories populaires ayant plus de 10000 occurrences
val motsRepetitifs = occurences  
  .filter($"count" >= 10000)
  .withColumnRenamed("mot", "mot_populaire")

val motsRepetitifsWithAlias = motsRepetitifs.as("motsRepetitifs")
// Étape 3: Créer un DataFrame avec les catégories moins populaires ayant entre 0 et 9999 occurrences
val motsMoinsPopulaires = occurences.filter($"count" < 10000).withColumnRenamed("mot", "mot_moins_populaire").as("motsMoinsPopulaires")

// Étape 4: Joindre businessTable avec motsRepetitifs et motsMoinsPopulaires pour associer les catégories populaires et moins populaires aux business
val businessWithType = businessWithCategoryList
  .join(motsRepetitifs, businessWithCategoryList("category_list") === motsRepetitifs("mot_populaire"), "left")
  .join(motsMoinsPopulaires, businessWithCategoryList("category_list") === motsMoinsPopulaires("mot_moins_populaire"), "left")
  .withColumn("category_type", 
    when($"motsRepetitifs.count".isNotNull, $"motsRepetitifs.mot_populaire")
    .otherwise(when($"motsMoinsPopulaires.count".isNotNull, $"motsMoinsPopulaires.mot_moins_populaire")
    .otherwise("unknown")))
// Étape 5: Aplatir les listes pour comparer les occurrences (maximum) de chaque catégorie
val businessWithMaxType = businessWithType
  .groupBy("business_id") // Group by business_id
  .agg(
    collect_list("type").alias("all_categories"),  // Adjust this if needed
    collect_list("motsRepetitifs.count").alias("popular_count"), 
    collect_list("motsMoinsPopulaires.count").alias("less_popular_count")
  )
// Étape 6: Trouver la catégorie ayant le nombre d'occurrences maximum parmi les populaires et les moins populaires
val businessTypeFinal = businessWithMaxType
  .withColumn("type", 
    when($"max_popular_count" >= $"max_less_popular_count", $"motsRepetitifs.mot_populaire")
    .otherwise($"motsMoinsPopulaires.mot_moins_populaire")) // Assigner la catégorie ayant le max de count

// Étape 7: Joindre le DataFrame final avec les autres informations de business
val businessTableWithType = businessTable
  .join(businessTypeFinal, Seq("business_id"), "left")  // Effectuer une jointure gauche sur business_id

// Étape 8: Ajouter les autres colonnes nécessaires
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
    "type"  // Ajoute la colonne `type`
  )
  
businessWithAllColumns.show(100, truncate = false)

//businessWithAllColumns.show(100, truncate= false)


// 6. Afficher le résultat
//businessTypeFinal.show(1000, truncate = false)

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
//businessWithAllColumns.write.mode("overwrite").jdbc(oracleUrl, "business", oracleProperties)
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



#### Error stacktrace:

```
scala.collection.LinearSeqOps.apply(LinearSeq.scala:129)
	scala.collection.LinearSeqOps.apply$(LinearSeq.scala:128)
	scala.collection.immutable.List.apply(List.scala:79)
	dotty.tools.dotc.util.Signatures$.applyCallInfo(Signatures.scala:244)
	dotty.tools.dotc.util.Signatures$.computeSignatureHelp(Signatures.scala:101)
	dotty.tools.dotc.util.Signatures$.signatureHelp(Signatures.scala:88)
	dotty.tools.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:47)
	dotty.tools.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:422)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: -1