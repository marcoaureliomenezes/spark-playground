package spark.batch

import config.Settings.fileCarsJSON
import org.apache.spark.sql.SparkSession
import spark.batch.my_utils.Utils.getSparkSession

object SparkSQL {

  val spark: SparkSession = getSparkSession("dev")


  def main(args: Array[String]): Unit = {

    val carsDF = spark.read
      .option("inferSchema", "true")
      .json(fileCarsJSON)

    carsDF.createOrReplaceTempView("cars")
    spark.sql("SELECT * FROM cars").show(5, truncate=false)
    spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
    spark.sql("SHOW DATABASES").show()

  }


}
