name := "spark-labs"

version := "0.2"

scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"
val postgresVersion = "42.7.1"
val cassandraConnectorVersion = "3.4.1"
val akkaVersion = "2.8.5"
val akkaHttpVersion = "10.5.3"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.23.1"
val typeConfigVersion = "1.4.3"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // akka
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,


  // spark avro
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,


  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,

  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % typeConfigVersion
)