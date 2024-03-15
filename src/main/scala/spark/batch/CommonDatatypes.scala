package spark.batch

import config.Settings.fileMoviesJSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import spark.batch.my_utils.Utils.getSparkSession

object CommonDatatypes {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = getSparkSession("dev")
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(fileMoviesJSON)

    // adding a plain value to df
    moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

    // Booleans
    val dramaFilter = col("Major_Genre") === "Drama"
    val goodRatingFilter = col("IMDB_Rating") > 6.9
    val preferredFilter = dramaFilter and goodRatingFilter
    moviesDF.select(col("Title"), preferredFilter.as("good_movie")).show()
    moviesDF.select("Title", "IMDB_Rating").where(preferredFilter).show()

    // Numbers
    moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2).show()

    def getCarNames: List[String] = List("Volkswagen", "Ford", "Mercedes-Bens")
    val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")

  }
}
