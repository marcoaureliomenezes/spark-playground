package spark.batch.dataframes

import org.apache.spark.sql.SparkSession
import spark.batch.my_utils.Utils.getSparkSession


object DataFramesFromScratch {

  val spark: SparkSession = getSparkSession("hml")
  import spark.implicits._

  // create DF from tuples
  val carsColumns = Seq("name", "MPG", "Cylinders", "Displacemet", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  val carsData = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  def main(args: Array[String]): Unit = {

    val carsDF = carsData.toDF(carsColumns: _*)
    carsDF.show
    carsDF.printSchema
  }
}
