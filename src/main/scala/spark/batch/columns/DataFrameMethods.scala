package spark.batch.columns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object DataFrameMethods extends App {

  val spark = SparkSession.builder()
    .appName("Spark Essentials")
    .config("spark.master", "local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  private val path_input = "data/csv/cidades.csv"

  private val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(path_input)

  val renameColumns = List(
    col("id"),
    col("city").as("cidade"),
    col("country").as("pais"),
    col("area"),
    col("population").as("população"),
    col("mayor").as("prefeito"),
    col("language").as("idioma")
  )

  val dfFinal = df.select(renameColumns: _*)
    .withColumn("densidade", col("população") / col("area"))
    .filter(col("população") / col("area") > 10000.0)

  val dfFinalUpper = dfFinal.toDF(dfFinal.columns.map(_.toUpperCase): _*)

  dfFinalUpper.show()
  dfFinalUpper.printSchema()
}