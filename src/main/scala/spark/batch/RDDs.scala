package spark.batch

import config.Settings.fileStocksCSV
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import spark.batch.my_utils.Utils.getSparkSession

import scala.io.Source

object RDDs {

  val spark: SparkSession = getSparkSession("dev")
  val sc: SparkContext = spark.sparkContext

  // 1- parallelize
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2- read from files
  case class Stock(symbol: String, date: String, price: Double)

  def readStocks(filename: String): List[Stock] =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => Stock(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks(fileStocksCSV))

  // 2b - read from files
  val stocksRDD2 = sc.textFile(fileStocksCSV)
    .map(line => line.split(","))
    .map(tokens => Stock(tokens(0), tokens(1), tokens(2).toDouble))

  // 3- Read from a dataframe

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(fileStocksCSV)

  val stocksRDD3 = stocksDF.rdd.map(row => Stock(row.getString(0), row.getString(1), row.getDouble(2)))

  import spark.implicits._
  val stocksDS = stocksDF.as[Stock]
  stocksDS.show(5)


  def main(args: Array[String]): Unit = {
    println("Hello RDDs")
  }

}
