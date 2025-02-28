```scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StarSchemaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Star Schema Example")
      .master("local[*]")
      .getOrCreate()

    // Création d'un DataFrame avec les colonnes de la table des faits (Reviews)
    val reviews = spark.createDataFrame(Seq(
      (1, 101, 5, 1001, 20230101, 10001),
      (2, 102, 4, 1002, 20230102, 10002)
    )).toDF("review_id", "user_id", "stars", "business_id", "date_id", "location_id")

    // Création des DataFrames pour les dimensions
    val users = spark.createDataFrame(Seq(
      (101, "Alice", "2020-01-01", 120, 4.5f, 50, 10),
      (102, "Bob", "2019-11-15", 90, 4.0f, 30, 8)
    )).toDF("user_id", "name", "yelping_since", "review_count", "average_stars", "fans", "compliment_cool")

    val businesses = spark.createDataFrame(Seq(
      (1001, "Restaurant A", "Food, Service", "WiFi, Parking", 150, 4.2f),
      (1002, "Cafe B", "Food, Drinks", "Parking", 200, 4.5f)
    )).toDF("business_id", "name", "categories", "attributes", "review_count", "stars")

    val dates = spark.createDataFrame(Seq(
      (20230101, 2023, 1, "Monday"),
      (20230102, 2023, 1, "Tuesday")
    )).toDF("date_id", "year", "month", "day_of_week")

    val locations = spark.createDataFrame(Seq(
      (10001, "Paris", "Île-de-France", "75001", 48.8566, 2.3522),
      (10002, "Lyon", "Auvergne-Rhône-Alpes", "69001", 45.7640, 4.8357)
    )).toDF("location_id", "city", "state", "postal_code", "latitude", "longitude")

    // Exemple d'utilisation de withColumn pour manipuler les colonnes

    val enrichedReviews = reviews
      .withColumn("stars_review", col("stars") * 2)  // Exemple : Créer une colonne avec les étoiles doublées
      .withColumn("user_name", users.filter(col("user_id") === col("user_id")).select("name").first().getString(0)) // Joindre un nom d'utilisateur
      .withColumn("business_name", businesses.filter(col("business_id") === col("business_id")).select("name").first().getString(0)) // Joindre un nom de commerce
      .withColumn("date_year", dates.filter(col("date_id") === col("date_id")).select("year").first().getInt(0)) // Joindre l'année de la date
      .withColumn("location_city", locations.filter(col("location_id") === col("location_id")).select("city").first().getString(0)) // Joindre la ville

    enrichedReviews.show()
  }
}
