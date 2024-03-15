package org.dadaia.spark.streaming.streaming4Advanced

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, length, sum, window}

object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def aggregateByProcessingTime() = {
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value"), current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"), "10 seconds").as("time"))
      .agg(sum(length(col("value"))).as("sumLength"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("sumLength")
      )

    linesDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }
}