package org.dadaia.spark.streaming.streaming1Basics

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}

object StreamingJoinStatic {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val guitarPlayers = spark.read.option("inferSchema", true).json("src/main/resources/data/guitarPlayers")
  val guitars = spark.read.option("inferSchema", true).json("src/main/resources/data/guitars")
  val bands = spark.read.option("inferSchema", true).json("src/main/resources/data/bands")

  val bandsSchema: StructType = bands.schema

  def joinStreamWithStatic() = {

    // Read stream from the console
    val streamingGuitarPlayers = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // Join with the static DataFrame
    val joinCondition: Column = guitarPlayers.col("id") === streamingGuitarPlayers.col("id")
    val guitaristsBands = guitarPlayers.join(streamingGuitarPlayers, joinCondition, "inner")

    guitaristsBands.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def joinStreamWithStream() = {
    // Read stream from the console
    val streamingGuitarPlayers = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamingBands = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // Join with the static DataFrame
    val joinCondition: Column = streamingBands.col("id") === streamingGuitarPlayers.col("id")
    val guitaristsBands = guitarPlayers.join(streamingGuitarPlayers, joinCondition, "inner")

    guitaristsBands.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    guitarPlayers.show()
    joinStreamWithStatic()
  }

  /**
   * Restrictions on joins with streaming DataFrames/Datasets:
   * stream joining with static: RIGHT OUTER, join/full outer join/right_semi join not supported
   * Only append supported as output mode for stream vs stream joins
   *

   */

}
