package spark.batch.columns

import config.Settings.fileStocksCSV
import org.apache.spark.sql.SparkSession

object ColumnOperators {

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

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(fileStocksCSV)

    df.show
  }
}
