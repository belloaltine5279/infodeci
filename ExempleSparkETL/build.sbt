name := "SparkETL"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.1.1"
)

// Configuration JVM
javaOptions ++= Seq(
  "-Xmx32G",                    // Mémoire maximale
  "-XX:+UseG1GC",               // Garbage Collector G1
  "-XX:InitiatingHeapOccupancyPercent=35", // Déclenchement du GC à 35% d'occupation
  "-XX:-UseGCOverheadLimit" ,    // Désactive la limite GC overhead
  "-Dspark.executor.memory=32g", // Mémoire pour les exécuteurs
  "-Dspark.driver.memory=32g",   // Mémoire pour le driver
)


// Exécuter Spark en mode forké
Compile / run / fork := true

// // Exécuter Spark en mode cluster 
// Compile / run / javaOptions ++= Seq(
//   "-Dspark.master=local[4]",
//   "-Dspark.executor.memory=16g"
// )
