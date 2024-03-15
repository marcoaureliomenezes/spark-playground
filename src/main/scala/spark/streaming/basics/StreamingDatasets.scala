package org.dadaia.spark.streaming.streaming1Basics

import Utils.Utils.getSparkSession
import common.{Car, carsSchema}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}

object StreamingDatasets {



  val spark: SparkSession = getSparkSession("local")
  import spark.implicits._

  def readCars(): Dataset[Car] = {

    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car]

    val cars: Dataset[Car] = spark.readStream
      .schema(carsSchema)
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.Name as Name", "car.Horsepower as Horsepower")
      .as[Car]
    cars
  }

  def showCarNames() = {

    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carsNamesDF: DataFrame = carsDS.select(col("Name"))

    // collection transformations maintain type info
    val carsNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carsNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // Read stream from the console
  }

}
