package org.dadaia.spark.dataframes

import config.Settings.{postgresDriver, postgresPassword, postgresURL, postgresUser}
import org.apache.spark.sql.SparkSession
import org.dadaia.spark.my_utils.Utils.getSparkSession

object DataSourcesSystems {

  val spark: SparkSession = getSparkSession("dev")
  import spark.implicits._

  // Reading from a remote DB
  spark.read
    .format("jdbc")
    .option("driver", postgresDriver)
    .option("url", postgresURL)
    .option("user", postgresUser)
    .option("password", postgresPassword)
    .option("dbtable", "public.employees")
    //.load().show

  def main(args: Array[String]): Unit = {
    println("Hello, DataSourcesSystems!")
  }

}
