package spark.batch

import config.Settings.fileMoviesJSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}
import spark.batch.my_utils.Utils.getSparkSession

object ManagingNulls {

  val spark: SparkSession = getSparkSession("dev")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(fileMoviesJSON)


    // select the first non-null value
    moviesDF.select(
      $"Title",
      $"Rotten_Tomatoes_Rating",
      $"IMDB_Rating",
      coalesce($"Rotten_Tomatoes_Rating", $"IMDB_Rating" * 10).as("Total_Gross")
    ).show()

    // checking for nulls
    moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull).show()

  // null when ordering
    moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last).show()

    // removing nulls
    moviesDF.select("Title", "IMDB_Rating").na.drop().show()

    // replacing nulls
    moviesDF.select("Title", "IMDB_Rating").na.fill(0).show()

    // Replacing nulls with different values using map
    moviesDF.na.fill(Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 0,
      "Director" -> "Unknown"
    )).show()

    // complex operations

    moviesDF.selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Total_Gross",        // same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Total_Gross_2",         // same as coalesce
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Total_Gross_3",      // returns null if the two values are equal
      "nvl2(Rotten_Tomatoes_Rating, 'Good', 'Bad') as Rating"                   // if the first value is not null, return the second value, else return the third value
    ).show()
  }
}
