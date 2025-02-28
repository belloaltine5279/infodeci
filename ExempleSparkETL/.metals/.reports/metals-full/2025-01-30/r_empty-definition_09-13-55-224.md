error id: `<none>`.
file://<WORKSPACE>/src/main/scala/SimpleApp.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
|empty definition using fallback
non-local guesses:
	 -

Document text:

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object SimpleApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
		
		// val usersFile = "/home3/ma618349/M2/logic/travail/donnee/yelp_academic_dataset_user.json"
		val businessFile = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_business.json"
		val businessCheckinFile = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_checkin.json"
		// Chargement du fichier JSON
		val  business = spark.read.json(businessFile).cache()
		val businesscheckin = spark.read.json(businessCheckinFile).cache()
		// Changement du type d'une colonne
		/*
		var business_table = business.withColumn("restaurant_name", col("").cast(DateType))
		business_table = business.withColumn("rating" , col("stars")) 
		business_table = business.withColumn("city", col("city"))
		business_table = business.withColumn()
		*/
		
		val table_des_faits = business.select("business_id")
		table_des_faits.

		

		// Extraction des amis, qui formeront une table "user_id - friend_id"
		//var friends = users.select("user_id", "friends")
		//friends = friends.withColumn("friends", explode(org.apache.spark.sql.functions.split(col("friends"), ",")))
		//friends = friends.withColumnRenamed("friends", "friend_id")

		/*
		// Pour supprimer les lignes sans friend_id
		friends = friends.filter(col("friend_id").notEqual("None"))
		
		// Suppression de la colonne "friends" dans le DataFrame users
		users = users.drop(col("friends"))
		
		// Extraction des années en temps qu'"élite", qui formeront une table "user_id - year"
		var elite = users.select("user_id", "elite")
		elite = elite.withColumn("elite", explode(org.apache.spark.sql.functions.split(col("elite"), ",")))
		elite = elite.withColumnRenamed("elite", "year")
		// Pour supprimer les lignes sans year
		elite = elite.filter(col("year").notEqual(""))
		elite = elite.withColumn("year", col("year").cast(IntegerType))
		
		// Suppression de la colonne "elite" dans le DataFrame users
		users = users.drop(col("elite"))
		
		// Affichage du schéma des DataFrame
		users.printSchema()
		friends.printSchema()
		elite.printSchema()
		
		val reviewsFile = "/home3/ma618349/M2/logic/travail/donnee/yelp_academic_dataset_review.json"
		// Chargement du fichier JSON
		var reviews = spark.read.json(reviewsFile).cache()
		// Changement du type d'une colonne
		reviews = reviews.withColumn("date", col("date").cast(DateType))
		
		// Affichage du schéma du DataFrame
		reviews.printSchema()
		
		*/
		// Paramètres de la connexion BD
		//Class.forName("org.postgresql.Driver")
		val url = "jdbc:postgresql://stendhal:5432/tpid2020"
		import java.util.Properties
		val postgresProperties = new Properties()
		postgresProperties.setProperty("driver", "")
		postgresProperties.setProperty("user", "tpid")
		postgresProperties.setProperty("password","tpid")

		var business_table = business.select("business_id", "name" ,"city" , "stars",  "categories") 
		businees
		//business_table = business_table.withColumn("name" , col("named"))
		//val business_checkin_table = businesscheckin.select("business_id", "date")

		val csv_business_dir = "<HOME>/M2/logic/travail/donnee/yelp_academic_dataset_tip.csv"
		val df:DataFrame = spark.read
			.option("header", "true")
			.option("inferSchema", "true")
			.csv(csv_business_dir)
		//df.show()

		val business_csv = df.select("business_id","date","user_id")
		df.select("")
/*
		val query_users = "select * from yelp.users" 
		val query_friends = "select * from yelp.friend " 
		val query_reviews= "select * from yelp.elite"

		val users_postgres = spark.read.jdbc(url , query_users , postgresProperties)
		val friends_postgres = spark.read.jdbc(url, query_friends , postgresProperties)
		val reviews_postgres = spark.read.jdbc(url, query_reviews, postgresProperties)

		val users = users_postgres.select("average_starts", "users_id") 
		val friends = friends_postgres.select("user_id", "friend_id ") 
		val reviews = reviews_postgres.select("business_id" , "date", "review_id", "stars" , "user_id")  
*/
		//Paramètre de la connexion de la data ware-house

		val ulroracle = "jdbc:oracle:thin:@stendhal:1521/enss2024" // Remplacez par le nom de votre table Oracle
		val jdbcProperties = new java.util.Properties()
		jdbcProperties.setProperty("user", "ma618349") // Remplacez par votre utilisateur Oracle
		jdbcProperties.setProperty("password", "ma618349") // Remplacez par votre mot de passe Oracle
		jdbcProperties.setProperty("driver", "oracle.jdbc.OracleDriver")

		//business_checkin_table.write.mode("append").jdbc(ulroracle , "business", jdbcProperties) // on utilise overwrite pour remplacer les données existantes dans la table
		business_table.write.mode("append").jdbc(ulroracle, "business", jdbcProperties) 
		//business_csv.write.mode("append").jdbc(ulroracle, "business_csv", jdbcProperties)
		

		//users.write.mode("append").jdbc(ulroracle, "users", jdbcProperties)
		//friends.write.mode("append").jdbc(ulroracle , "friends", jdbcProperties)
		//reviews.write.mode("append").jdbc(ulroracle , "reviews", jdbcProperties) 

		
		// Enregistrement du DataFrame users dans la table "user"
		/*
		users.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.\"user\"", connectionProperties)
		
		// Enregistrement du DataFrame friends dans la table "friend"
		friends.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.friend", connectionProperties)
		// Enregsitrement du DataFrame elite dans la table "elite"
		elite.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.elite", connectionProperties)
		
		// Enregistrement du DataFrame reviews dans la table "review"
		reviews.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "yelp.review", connectionProperties)
		*/


		spark.stop()
	}
}

```

#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.