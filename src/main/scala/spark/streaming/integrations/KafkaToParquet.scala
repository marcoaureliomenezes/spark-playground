package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import common.{carsSchema, clientSchema}
import config.Settings.{kafkaCluster, outputClientParquet, pathCheckpoint, topicRawClients}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.dadaia.spark.streaming.streaming3Integrations.KafkaToConsole.spark

object KafkaToParquet {

  val spark: SparkSession = getSparkSession("local")


    def kafkaToParquet() = {

      // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
      val kafkaDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaCluster)
        .option("subscribe", topicRawClients)
        .load()

      val parsedDF = kafkaDF.select(col("topic"), col("value").cast(StringType).as("value"))
        .withColumn("data", from_json(col("value"), clientSchema))
        .select("data.*")

      parsedDF
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", pathCheckpoint)
        .option("path", outputClientParquet)
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .start()
        .awaitTermination()
    }

  def main(args: Array[String]): Unit = {
    kafkaToParquet()
  }
}