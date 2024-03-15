package org.dadaia.spark.streaming.streaming2LowLevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import SocketsDStreams.spark
import Utils.Utils.getSparkSession
import config.Settings.inputStocksCSV

import java.io.File
import java.sql.Date
import java.text.{ParseException, SimpleDateFormat}
import java.time.format.DateTimeFormatter

object FilePathDStreams {


  val spark: SparkSession = getSparkSession("JDBCToKafka")
  val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(1))


  private def readFromFile(): Unit = {
    createNewFile()
    val fileStream: DStream[String] = ssc.textFileStream(inputStocksCSV)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val stocksStream: DStream[Stock] = fileStream.map { line =>
        val tokens = line.split(",")
        val company = tokens(0)
        val date = new Date(dateFormat.parse(tokens(1)).getTime)
        val price = tokens(2).toDouble
      Stock(company, date, price)
    }
    // action
    stocksStream.print()
    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromFile()
  }

  private def createNewFile(): Unit = {
    new Thread(() => {
      val path = inputStocksCSV

      Thread.sleep(5000)
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()
      val writer = new java.io.PrintWriter(newFile)
      writer.write("""
                     |AAPL,2023-10-12,25.94
                     |AAPL,2023-10-12,28.66
                     |AAPL,2023-10-12,33.95
                     |AAPL,2023-10-12,31.01
                     |AAPL,2023-10-12,21
                     |AAPL,2023-10-12,26.19
                     |AAPL,2023-10-12,25.41
                     |AAPL,2023-10-12,30.47
                     |AAPL,2023-10-12,12.88""".stripMargin.trim)
      writer.close()
    }).start()
  }
}
