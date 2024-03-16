package org.dadaia.spark.streaming.streaming4Advanced

import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import spark.my_utils.Utils.getSparkSession

import java.net.ServerSocket
import java.sql.Timestamp
import scala.concurrent.duration.DurationInt

object Watermarks {


  val spark: SparkSession = getSparkSession("Streaming Advanced - Watermarks")
  import spark.implicits._


  def debugQuery(query: StreamingQuery) = {
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString
        println(s"$i: $queryEventTime")
      }
    })
  }

  def testWatermarks(): Unit = {
    val dataDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()
      .as[String]
      .map {
        line =>
          val tokens = line.split(",")
          val timestamp = new Timestamp(tokens(0).toLong)
          val data = tokens(1)
          (timestamp, data)
      }.toDF("created", "color")

    val watermarkedDF: DataFrame = dataDF
      .withWatermark("created", "2 seconds")
      .groupBy(window($"created", "2 seconds"), $"color")
      .count()
      .selectExpr("window.*", "color", "count")

    val query = watermarkedDF
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)     // Useful skill for debuging
    query.awaitTermination()
  }
  // 3000,blue

  def main(args: Array[String]): Unit = {

    testWatermarks()
  }
}

object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept()  // Wait for the client to connect
  val printer = new java.io.PrintWriter(socket.getOutputStream, true)

  println("Connected to the client")

  def example1(): Unit = {
    (1 to 100).foreach { i =>
      Thread.sleep(1000)
      val now = System.currentTimeMillis()
      printer.write(s"$now,blue\n")
      printer.flush()
    }
  }

  def main(args: Array[String]): Unit = {
    example1()
  }
}
