package spark.streaming.basics

import config.Settings.{socketHost, socketInPort1}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, from_json, sum, to_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import spark.my_utils.Utils.getSparkSession

object Aggregations {

  val spark: SparkSession = getSparkSession("Spark Streaming: Aggregations")

  def streamingCount() = {
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()

    val linesCountDF: DataFrame = linesDF.selectExpr("count(*) as lineCount")

    val query: StreamingQuery = linesCountDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val clientSchema = StructType(Array(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("saldo", DoubleType)
    ))

    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()

    val parsedDF: DataFrame = linesDF
      .withColumn("value", from_json(col("value"), clientSchema))
      .select(
        col("value.id").as("id"),
        col("value.name").as("nome"),
        col("value.age").as("idade"),
        col("value.saldo").as("saldo")
      )
    val aggregationDF = parsedDF.select(aggFunction(col("saldo")).as("agg_so_far"))
    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketInPort1)
      .load()

    val names = linesDF.select(col("value").as("name"))
    val nameGroups = names.groupBy(col("name")).agg(count("*").as("groupCount"))

    nameGroups.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    // streamingCount()
    numericalAggregations(avg)
    // groupNames()
  }

}
