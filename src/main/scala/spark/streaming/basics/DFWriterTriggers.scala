package org.dadaia.spark.streaming.streaming1Basics

import Utils.Utils.getSparkSession
import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

object DFWriterTriggers {

  val spark: SparkSession = getSparkSession("DataStream Writer Triggers")
  val socketLines: DataFrame = spark.readStream
    .format("socket")
    .option("host", socketHost)
    .option("port", socketInPort1)
    .load()

    socketLines.writeStream.format("console").outputMode("append")
    .trigger(Trigger.Once())                                            // single batch, then terminate
    // .trigger(Trigger.ProcessingTime(2.seconds))                      // every 2 seconds run the query
    // .trigger(Trigger.Continuous(2.seconds))                          // experimental, continuous processing
    .start()
    .awaitTermination()

  def main(args: Array[String]): Unit = {
    println("Hello, Streaming DataFrames!")
  }
}
