package org.dadaia.spark.streaming.streaming2LowLevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import FilePathDStreams.spark
import Utils.Utils.getSparkSession
import config.Settings.{outputWordsTXT, socketHost, socketInPort1}

object SocketsDStreams {


  val spark: SparkSession = getSparkSession("JDBCToKafka")
  val ssc: StreamingContext  = new StreamingContext(spark.sparkContext, Seconds(1))

  def readFromSocket(): Unit = {
    val socketStream: DStream[String] = ssc.socketTextStream(socketHost, socketInPort1)
    val wordsStream : DStream[String] = socketStream.flatMap(line => line.split(" "))
    wordsStream.saveAsTextFiles(outputWordsTXT)
    ssc.start()
    ssc.awaitTermination()  // block the current thread
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }
}
