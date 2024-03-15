package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import common.stocksSchema
import config.Settings.inputStocksCSV
import org.apache.spark.sql.SparkSession

object FilesToConsole {

  val spark: SparkSession = getSparkSession("local Directory Consumer")

  def readFromFiles() = {
    val stocksDF = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load(inputStocksCSV)

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    println("Hello, Streaming DataFrames!")
    readFromFiles()
  }

}
