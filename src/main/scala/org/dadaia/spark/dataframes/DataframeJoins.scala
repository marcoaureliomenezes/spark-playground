package org.dadaia.spark.dataframes

import config.Settings.{fileBandsJSON, fileGuitarPlayersJSON, fileGuitarsJSON}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.dadaia.spark.my_utils.Utils.getSparkSession

object DataframeJoins {

  val spark: SparkSession = getSparkSession("hml")
  import spark.implicits._

  // READ JSON FILE MOVIES
  val guitarsDF = spark.read.option("inferSchema", "true").json(fileGuitarsJSON)

  // READ JSON FILE MOVIES
  val guitaristsDF = spark.read.option("inferSchema", "true").json(fileGuitarPlayersJSON)

  // READ JSON FILE MOVIES
  val bandsDF = spark.read.option("inferSchema", "true").json(fileBandsJSON)

  // JOINS
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

  // INNER JOINS
  guitaristsDF.join(bandsDF, joinCondition, "inner").show

  // OUTER JOINS
  // left outer = everything in the inner join + all the rows in the LEFT table, with NULLs in where the data is missing.
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing.
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing.
  guitaristsDF.join(bandsDF, joinCondition, "outer").show

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition.
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show

  // anti-joins = everything in the left DF for which there is NO a row in the right DF satisfying the condition.
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show


  // THINGS TO HAVE IN MIND USING JOINS
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  // guitaristsBandsDF.select("id", "band").show       CRASHES BECAUSE OF AMBIGUOS
  // Option 1 - Rename the Column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  // Option 2 - drop the dupe column
  guitaristsDF.drop(bandsDF.col("id"))
  // Option 3 - Rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandID")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitar, guitarId"))

}
