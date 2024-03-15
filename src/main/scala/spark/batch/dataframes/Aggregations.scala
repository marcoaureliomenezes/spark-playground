package spark.batch.dataframes

import config.Settings
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, max, min, stddev, sum}

object Aggregations {

  private def getSparkSession(runEnvironment: String): SparkSession = {
    println(s"Creating SparkSession for $runEnvironment")
    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

//  private def getSparkConfigs(runEnvironment: String) = {
//    val conf = Settings.source
//
//  }


  val BASE_PATH = "src/main/resources/data"
  val MOVIES_JSON = s"${BASE_PATH}/movies.json"
  val spark = getSparkSession("dev")
  import spark.implicits._

  // READ JSON FILE MOVIES
  val moviesDF = spark.read.option("inferSchema", "true").json(MOVIES_JSON)

  // Aggregation Functions
  moviesDF.select(
    count(col("Major_Genre")).as("num_genres"),
    countDistinct(col("Major_Genre")).as("num_distinct_genres"),
    approx_count_distinct(col("Major_Genre")).as("aprox_num_distinct_genres"),
    min(col("IMDB_Rating")).alias("Minimum_IMDB_Rating"),
    max(col("IMDB_Rating")).alias("Maximum_IMDB_Rating"),
    sum(col("IMDB_Rating")).alias("Sum_IMDB_Rating"),
    avg(col("IMDB_Rating")).alias("Average_IMDB_Rating"),
    stddev(col("IMDB_Rating")).alias("stddev_IMDB_Rating")
  ).show()

  // Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .count() // SELECT COUNT(*) FROM moviesDF GROUP BY Major_Genre

  val abgRatingDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating") // SELECT COUNT(*) FROM moviesDF GROUP BY Major_Genre

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()
//
//  moviesDF.select()
//  moviesDF.selectExpr(
//    """count(Major_Genre)
//      |
//      |""".stripMargin)

  def main(args: Array[String]): Unit = {
    aggregationsByGenreDF.show()
  }

}
