package org.dadaia.spark.streaming.streaming3Integrations

import Utils.Utils.getSparkSession
import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import config.Settings.{cassandraKeyspace, cassandraTableCars, inputCarsJSON}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object KafkaToCassandra {

  val spark: SparkSession = getSparkSession("JDBCToKafka")
  import spark.implicits._

  private def kafkaToCassandra() = {

    val carsDS = spark.readStream
      .schema(carsSchema)
      .json(inputCarsJSON)
      .as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        batch
          .select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat(cassandraTableCars, cassandraKeyspace)
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    kafkaToCassandra()
  }
}
