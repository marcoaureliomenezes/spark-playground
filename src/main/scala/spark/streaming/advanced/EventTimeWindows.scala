package org.dadaia.spark.streaming.streaming4Advanced

import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Processing")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket(): DataFrame =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    aggregatePurchasesBySlidingWindow()

  }

}
