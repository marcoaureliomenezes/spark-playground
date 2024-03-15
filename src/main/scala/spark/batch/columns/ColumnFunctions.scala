package spark.batch.columns

import config.Settings.fileCarsJSON
import org.apache.spark.sql.{DataFrame, SparkSession}

object ColumnFunctions {

  private def getSparkSession(runEnvironment: String): SparkSession = {
    println(s"Creating SparkSession for $runEnvironment")
    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = getSparkSession("hml")
    import spark.implicits._

    val df: DataFrame = spark.read
      .option("inferSchema", "true")
      .json(fileCarsJSON)

    df.show

  }
}