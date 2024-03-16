package org.dadaia.spark.streaming.streaming3Integrations

import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import spark.my_utils.Utils.getSparkSession

import scala.concurrent.duration.DurationInt

object SocketToConsole {

  val spark: SparkSession = getSparkSession("Spark Streaming: SocketConsumer")

  def socketToConsole() = {
    val socketLines: DataFrame = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()


    socketLines.writeStream
      .format("console")
      .outputMode("append")
      // .trigger(Trigger.ProcessingTime(2.seconds)) // A cada 2 segundos cria um batch com o que tiver
      // .trigger(Trigger.AvailableNow()) // Cria um batch com o que tiver disponível
      .trigger(Trigger.Continuous(2.seconds)) // A cada 2 segundos cria um batch com o que tiver mesmo que não tenha nada
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    println("Hello, Streaming DataFrames!")
    socketToConsole()
  }
}
