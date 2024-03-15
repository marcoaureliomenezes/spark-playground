package config

import com.typesafe.config.{Config, ConfigFactory}

object Settings {

  val config: Config = ConfigFactory.load()

  val basePath: String = config.getString("src.file.basePath")
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

  // JDBC CONNECTIONS
  val postgresDriver: String = config.getString("src.jdbc.postgres.driver")
  val postgresURL: String = config.getString("src.jdbc.postgres.url")
  val postgresUser: String = config.getString("src.jdbc.postgres.user")
  val postgresPassword: String = config.getString("src.jdbc.postgres.password")
}
