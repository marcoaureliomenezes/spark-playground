package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import common.{carsSchema, clientSchema}
import config.Settings.{kafkaCluster, outputClientParquet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.dadaia.spark.streaming.streaming3Integrations.KafkaToParquet.spark

object FilesToKafka {

  val spark: SparkSession = getSparkSession("FilesToKafka")


  def filesToKafka() = {

    val clientProcessedDF = spark.readStream
      .schema(clientSchema)
      .parquet(outputClientParquet)

    val colNames = clientProcessedDF.columns.toSeq
    val structExpr = struct(colNames.map(col): _*).cast("string")
    val transformedDF = clientProcessedDF.withColumn("value", structExpr)
      .drop(colNames: _*)


    transformedDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCluster)
      .option("topic", "clients_processed")
      .outputMode("append")
      .trigger(ProcessingTime("2 seconds"))
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    filesToKafka()
  }

}
