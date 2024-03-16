package org.dadaia.spark.streaming.streaming3Integrations

import common.{Car, carsSchema}
import config.Settings.{inputCarsJSON, postgresDriver, postgresPassword, postgresUrl, postgresUser}
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.my_utils.Utils.getSparkSession

object KafkaToJDBC {

  val spark: SparkSession = getSparkSession("JDBCToKafka")
  import spark.implicits._


  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json(inputCarsJSON)

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", postgresDriver)
          .option("url", postgresUrl)
          .option("user", postgresUser)
          .option("password", postgresPassword)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
