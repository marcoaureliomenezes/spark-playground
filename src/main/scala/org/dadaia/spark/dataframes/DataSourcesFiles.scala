package org.dadaia.spark.dataframes

import config.Settings.{fileCarsAvro, fileCarsJSON, fileCarsParquet, fileStocksCSV, postgresDriver, postgresPassword, postgresURL, postgresUser}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.dadaia.spark.commons.MySchemas.{carsSchema, stocksSchema}
import org.dadaia.spark.my_utils.Utils.getSparkSession

object DataSourcesFiles {

  val spark: SparkSession = getSparkSession("dev")
  import spark.implicits._

  /*****************************************    READING JSON FILES    *************************************************/

  spark.read.format("json").schema(carsSchema)
    .option("mode", "failFast")
    .load(fileCarsJSON).show

  spark.read.format("json").schema(carsSchema)
    .options(Map(
      "mode" -> "failFast",
      "path" -> fileCarsJSON
    )).load().show


  val carsDF = spark.read.schema(carsSchema)                          // READING JSON PASSING MANY OPTIONS
    .option("dateFormat", "YYYY-MM-dd")                               // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")                            // bzip2, gzip, lz4, snappy, deflate
    .json(fileCarsJSON)

  carsDF.show()


  /*****************************************    READING csv FILES    **************************************************/
  spark.read.format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv(fileStocksCSV).show()



  /********************************    READING PARQUET AND AVRO FILES    **********************************************/

  // WRITING PARQUET FILES
  carsDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("dateFormat", "YYYY-MM-dd")
    .option("path", fileCarsParquet)
    .save()

  // WRITING AVRO FILES
   carsDF.write
     .format("avro")
     .mode(SaveMode.Overwrite)
     .option("dateFormat", "YYYY-MM-dd")
     .option("path", fileCarsAvro)
     .save()


  def main(args: Array[String]): Unit = {
    println("Data Sources")
  }
}
