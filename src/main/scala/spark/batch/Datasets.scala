package spark.batch

import config.Settings.{fileCarsJSON, fileNumbersCSV}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import spark.batch.my_utils.Utils.getSparkSession

object Datasets {

  val spark: SparkSession = getSparkSession("dev")
  import spark.implicits._

  def readJSON(path: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(path)
  }

  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(fileNumbersCSV)

  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.show()

   // Dataset of a complex car
  case class Car(
                    Name: String,
                    Miles_per_Gallon: Option[Double],
                    Cylinders: Long,
                    Displacement: Double,
                    Horsepower: Option[Long],
                    Weight_in_lbs: Long,
                    Acceleration: Double,
                    Origin: String,
                    Year: String
                  )

      val carsDS: Dataset[Car] =  readJSON(fileCarsJSON).as[Car]
      carsDS.show(5, truncate=false)

      // Count
      println(s"Number of cars: ${carsDS.count()}")

      // Filtering
      carsDS.filter(car => car.Horsepower.getOrElse(0L) > 150).show(5, truncate=false)

      // Mapping
      carsDS.map(car => (car.Name, car.Year)).show(5, truncate=false)


  def main(args: Array[String]): Unit = {


  }

}
