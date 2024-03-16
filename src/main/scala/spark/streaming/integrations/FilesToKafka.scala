package org.dadaia.spark.streaming.streaming3Integrations

import config.Settings.{kafkaCluster, outputClientParquet}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import _root_.spark.my_utils.Utils.getSparkSession

object FilesToKafka {

  val spark: SparkSession = getSparkSession("Streaming: FilesToKafka")


  def filesToKafka() = {

    val clientProcessedDF: DataFrame = spark.readStream
      //.schema(clientSchema)
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
