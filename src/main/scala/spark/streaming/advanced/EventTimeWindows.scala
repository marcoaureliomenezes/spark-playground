package org.dadaia.spark.streaming.streaming4Advanced

import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, split, sum, to_date, to_timestamp, window, count}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import spark.my_utils.Utils.getSparkSession

object EventTimeWindows {

  val spark: SparkSession = getSparkSession("Spark Streaming: Events Time Windows")

  private val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  private def readAPIKeysConsumeFromSocket(): DataFrame = {
    val logSchema = StructType (Array (
    StructField ("value", StringType),
    StructField ("timestamp", StringType)
    ))

   spark.readStream
  .format ("socket")
  .option ("host", socketHost)
  .option ("port", socketInPort1)
  .load ()
  .select (from_json (col ("value"), logSchema).as ("log") )
  .select ("log.*")
  .withColumn ("api_key", split (col ("value"), ";").getItem (1) )
  .withColumn ("timestamp", to_timestamp (col ("timestamp") ) )
  .select ("api_key", "timestamp")
  }


  private def aggregateAPIKeysConsumeByTumblingWindow() = {

    val apiKeysDF: DataFrame = readAPIKeysConsumeFromSocket()

    val windowByDay = apiKeysDF
      .groupBy(window(col("timestamp"), "1 day").as("time"), col("api_key").as("api_key_id")) // struct column: has fields {start, end}
      .agg(count("api_key").as("api_key_requests"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("api_key_requests"),
        col("api_key_id")
      )

    windowByDay.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregateAPIKeysConsumeBySlidingWindow() = {

    val apiKeysDF: DataFrame = readAPIKeysConsumeFromSocket()

    val windowByDay = apiKeysDF
      .groupBy(window(col("timestamp"), "1 day", "1 hour").as("time"), col("api_key").as("api_key_id")) // struct column: has fields {start, end}
      .agg(count("api_key").as("api_key_requests"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("api_key_requests"),
        col("api_key_id")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    aggregateAPIKeysConsumeBySlidingWindow()
    // aggregateAPIKeysConsumeByTumblingWindow()
  }

}
