package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object SocketToConsole {

  val spark: SparkSession = getSparkSession("SocketConsumer")

  def socketToConsole() = {
    val socketLines = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()



    socketLines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    println("Hello, Streaming DataFrames!")
    socketToConsole()
  }
}
