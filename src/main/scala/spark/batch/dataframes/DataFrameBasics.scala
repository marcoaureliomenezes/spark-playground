package spark.batch.dataframes

import config.Settings.fileCarsJSON
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.batch.commons.MySchemas.carsSchema
import spark.batch.my_utils.Utils.getSparkSession

object DataFrameBasics {

  val spark: SparkSession = getSparkSession("hml")
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val carsDF: DataFrame = spark.read.option("inferSchema", "true").json(fileCarsJSON) // Reading JSON inferring Schema
    carsDF.printSchema                                                                  // Printing Schema
    carsDF.take(3).foreach(println)                                                     // Printing first 3 row

    val carsDFWithSchema = spark.read.schema(carsSchema).json(fileCarsJSON)             // Reading with Schema
    carsDFWithSchema.printSchema                                                        // Printing Schema
  }
}
