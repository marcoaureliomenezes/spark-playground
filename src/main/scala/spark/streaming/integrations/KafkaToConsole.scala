package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import common.clientSchema
import config.Settings.kafkaCluster
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, to_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.dadaia.spark.streaming.streaming3Integrations.KafkaToParquet.spark

object KafkaToConsole {


  val spark: SparkSession = getSparkSession("kafkaToConsole")


  def kafkaToConsole() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCluster)
      .option("subscribe", "blockchain")
      .load()

    val parsedDF = kafkaDF.select(col("topic"), col("value").cast(StringType).as("value"))
      .withColumn("data", from_json(col("value"), clientSchema))
      .select("data.*")

    parsedDF
      .writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    kafkaToConsole()
  }
}
