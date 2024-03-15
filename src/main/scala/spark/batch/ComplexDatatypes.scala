package spark.batch

import config.Settings.{fileMoviesJSON, fileStocksCSV}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.batch.my_utils.Utils.getSparkSession

object ComplexDatatypes {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = getSparkSession("dev")

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(fileMoviesJSON)

    moviesDF.select("Title", "Release_Date")
          .withColumn("Actual_Release", to_date(col("Release_Date"), "dd-MMM-yy"))
      .withColumn("Today", current_date())
      .withColumn("Right_Now", current_timestamp())
      .withColumn("Movie_Age", datediff(current_date(), col("Actual_Release")) / 365) // date_add, date_sub
      .show(1)

    /**
     * Exercise:
     * 1. How to we deal with complex data formats
     * 2. Read the stocks DF and parse the dates
     */

    val stocksDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(fileStocksCSV)

    stocksDF.
      withColumn("date_parsed", to_date(col("date"), "MMM dd yyyy")).
      show(1)

    // Structures

    // 1: With Column Operators
    moviesDF.select(
        col("Title"),
        struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
      ).
      withColumn("Profit_extracted", col("Profit").getField("US_Gross") - col("Profit").getField("Worldwide_Gross")).
    show(5, truncate=false)

    // 2: With expression Strings
    moviesDF.selectExpr(
      "Title",
      "struct(US_Gross, Worldwide_Gross) as Profit"
    ).
      selectExpr(
        "Title",
        "Profit.US_Gross - Profit.Worldwide_Gross as Profit"
      ).
      show(5, truncate=false)

    // Arrays
    val moviesWithWords = moviesDF.select(
      col("Title"),
      split(col("Title"), " ").as("Title_Words")
    )

    moviesWithWords.select(
      col("Title"),
      expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    ).show(5, truncate=false)

  }


}
