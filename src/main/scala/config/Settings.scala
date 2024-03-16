package config

import com.typesafe.config.{Config, ConfigFactory}

object Settings {

  val config: Config = ConfigFactory.load()

  val basePath: String = config.getString("src.file.basePath")

  val inputBasePath = config.getString("base.path.inputs")
  val outputBasePath = config.getString("base.path.outputs")

  val fileCarsJSON: String = s"${basePath}/${config.getString("src.file.json.cars")}"
  val fileMoreCarsJSON: String = s"${basePath}/${config.getString("src.file.json.moreCars")}"
  val fileCarsDatesJSON: String = s"${basePath}/${config.getString("src.file.json.carsDates")}"

  val fileMoviesJSON: String = s"${basePath}/${config.getString("src.file.json.movies")}"
  val fileGuitarsJSON: String = s"${basePath}/${config.getString("src.file.json.guitars")}"
  val fileGuitarPlayersJSON: String = s"${basePath}/${config.getString("src.file.json.guitarPlayers")}"
  val fileBandsJSON: String = s"${basePath}/${config.getString("src.file.json.bands")}"

  val filePopulationJSON: String = s"${basePath}/${config.getString("src.file.json.population")}"

  // CSV FILE PATHS
  val fileStocksCSV: String = s"${basePath}/${config.getString("src.file.csv.stocks")}"
  val fileNumbersCSV: String = s"${basePath}/${config.getString("src.file.csv.numbers")}"

  // PARQUET FILE PATHS
  val fileCarsParquet: String = s"${basePath}/${config.getString("dst.file.parquet.cars")}"

  // AVRO FILE PATHS
  val fileCarsAvro: String = s"${basePath}/${config.getString("dst.file.avro.cars")}"

  /*******************************************************************/
  /**************************  FILE CONFIGS  *************************/



  val inputStocksCSV = s"${inputBasePath}/${config.getString("src.file.csv.stocks")}"
  val inputCarsJSON = s"${inputBasePath}/${config.getString("src.file.json.cars")}"

  val outputWordsTXT = s"${outputBasePath}/${config.getString("dst.file.txt.words")}"
  val pathCheckpoint = s"${config.getString("base.path.checkpoints")}"
  val outputClientParquet = s"${outputBasePath}/${config.getString("dst.file.parquet.clients")}"

  /*******************************************************************/
  /*************************  SOCKET CONFIGS  *************************/


  val socketHost = config.getString("socket.input.host")
  val socketInPort1 = config.getInt("socket.input.port_1")
  val socketOutPort1 = config.getInt("socket.output.port_1")

  /*******************************************************************/
  /*************************  KAFKA CONFIGS  *************************/

  val kafkaCluster = config.getString("kafka.cluster")

  // Kafka topics for the clients (raw, bronze, silver, gold)
  val topicRawClients = config.getString("kafka.topic.raw.clients")
  val topicBronzeClients = config.getString("kafka.topic.bronze.clients")
  val topicSilverClients = config.getString("kafka.topic.silver.clients")
  val topicGoldClients = config.getString("kafka.topic.gold.clients")

  // Kafka topics for transactions (raw, bronze, silver, gold)
  val topicRawTransactions = config.getString("kafka.topic.raw.transactions")
  val topicBronzeTransactions = config.getString("kafka.topic.bronze.transactions")
  val topicSilverTransactions = config.getString("kafka.topic.silver.transactions")
  val topicGoldTransactions = config.getString("kafka.topic.gold.transactions")

  /*******************************************************************/
  /***********************  POSTGRES CONFIGS  ************************/

  val postgresDriver = config.getString("postgres.driver")
  val postgresUrl = config.getString("postgres.url")
  val postgresUser = config.getString("postgres.user")
  val postgresPassword = config.getString("postgres.password")

  /*******************************************************************/
  /**********************  CASSANDRA CONFIGS  ************************/

  val cassandraKeyspace = config.getString("cassandra.keyspace")
  val cassandraTableCars = config.getString("cassandra.table.cars")

}
