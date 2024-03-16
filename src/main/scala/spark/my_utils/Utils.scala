package spark.batch.my_utils

import config.Settings.{basePath, postgresDriver, postgresPassword, postgresURL, postgresUser}
import exercises.DataFrameJoins.spark
import exercises.SparkSQL.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object Utils {


  def getSparkSession(runEnvironment: String): SparkSession = {
    println(s"Creating SparkSession for $runEnvironment")
    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def readJSON(spark: SparkSession, path: String): DataFrame = spark.read.option("inferSchema", "true").json(path)

  def readPGTable(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", postgresDriver)
      .option("url", postgresURL)
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("dbtable", s"public.$tableName")
      .load()
  }




  def getTransactionTime: String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-'T'HH:mm:ss.SSS'Z'")
    val saoPauloZone: ZoneId = ZoneId.of("America/Sao_Paulo")
    format.setTimeZone(TimeZone.getTimeZone(saoPauloZone))
    format.format(System.currentTimeMillis())
  }

  def deltaDays(dataString: String, dias: Int): String = {
    val formato: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date: LocalDate = LocalDate.parse(dataString, formato)
    val dataFormatada = date.plusDays(dias)
    dataFormatada.format(formato)
  }

}
