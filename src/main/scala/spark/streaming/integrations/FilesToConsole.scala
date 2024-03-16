package org.dadaia.spark.streaming.streaming3Integrations

import config.Settings.inputStocksCSV
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import spark.batch.commons.MySchemas.stocksSchema
import spark.my_utils.Utils.getSparkSession

import scala.concurrent.duration.DurationInt

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
      .trigger(Trigger.ProcessingTime(2.seconds)) // A cada 2 segundos cria um batch com o que tiver
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    println("Hello, Streaming DataFrames!")
    readFromFiles()
  }

}
