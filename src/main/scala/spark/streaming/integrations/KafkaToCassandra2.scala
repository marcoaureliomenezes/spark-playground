package org.dadaia.spark.streaming.streaming3Integrations

import com.datastax.spark.connector.cql.CassandraConnector
import config.Settings.{cassandraKeyspace, cassandraTableCars, inputCarsJSON}
import org.apache.spark.sql.SparkSession
import _root_.spark.my_utils.Utils.getSparkSession
import spark.batch.Datasets.Car

object KafkaToCassandra2 {

  val spark: SparkSession = getSparkSession("JDBCToKafka")
  import spark.implicits._

  class CarCassandraForeachWriter extends org.apache.spark.sql.ForeachWriter[Car] {

    val connector = CassandraConnector(spark.sparkContext.getConf)
    override def open(partitionId: Long, version: Long): Boolean = {
      println("Open connection")
      true
    }
    override def process(value: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |insert into $cassandraKeyspace.$cassandraTableCars("Name", "Horsepower")
             |values ('${value.Name}', ${value.Horsepower.orNull})
             |""".stripMargin
        )
      }
    }
    override def close(errorOrNull: Throwable): Unit = println("Close connection")
  }


  def writeToCassandra() = {

    val carsDS = spark.readStream
      //.schema()
      .json(inputCarsJSON)
      .as[Car]

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToCassandra()
  }


}
