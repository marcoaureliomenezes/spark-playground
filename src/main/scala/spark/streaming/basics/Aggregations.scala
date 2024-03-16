package org.dadaia.spark.streaming.streaming1Basics

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def streamingCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount = lines.selectExpr("count(*) as lineCount")
    // aggregations with distinct are not supported
    // Otherwise spark will need to keep track of all the unique values of the column
    val query = lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()

    query.awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines.select(col("value").as("name"))
    val nameGroups = names.groupBy(col("name")).agg(count("*").as("groupCount"))

    nameGroups.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    // streamingCount()
    // numericalAggregations(count)
    groupNames()
  }

}