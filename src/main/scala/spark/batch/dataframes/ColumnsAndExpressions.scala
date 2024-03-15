package spark.batch.dataframes

import config.Settings.{fileCarsJSON, fileMoreCarsJSON}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr}
import spark.batch.my_utils.Utils.getSparkSession

object ColumnsAndExpressions {

  val spark: SparkSession = getSparkSession("hml")
  import spark.implicits._

  // READ JSON FILE CARS
  val carsDF = spark.read.option("inferSchema", "true").json(fileCarsJSON)
  carsDF.show()


  // SELECTING
  val firstColumn: Column = carsDF.col("Name")
  val carNameDF = carsDF.select(firstColumn)
  carNameDF.show()

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to colum
    $"Horsepower", // Fancier interpolated string, returns a Column Object
    expr("Origin")
  ).show

  carsDF.select("Name", "Acceleration").show

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    simplestExpression.as("Weight_in_lbs_2"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  ).show

  // selectExpr
  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  ).show

  // DF processing
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2).show        // Adding a column
  carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds").show          // Renaming a column
  carsDF.selectExpr("Weight_in_lbs AS `Weight in pounds`").show
  carsDF.drop("Cylinders", "Displacement")                                    // remove a column

  // Filters
  carsDF.filter(col("Origin") =!= "USA").show
  carsDF.where(col("Origin") =!= "USA").show

  // filtering with expression strings
  carsDF.filter("Origin = 'USA'").show

  // chain filters
  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  carsDF.filter("Origin = 'USA' AND Horsepower > 150")

  // union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json()
  carsDF.union(moreCarsDF).show // works if the DFs have the same schema

  // distinct values
  carsDF.select("Origin").distinct().show

  def main(args: Array[String]): Unit = {
    spark.close()
  }
}
