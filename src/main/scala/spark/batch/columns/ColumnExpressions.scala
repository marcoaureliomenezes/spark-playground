package spark.batch.columns

import config.Settings
import config.Settings.filePopulationJSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, expr}
import org.apache.spark.sql.types.StringType




object ColumnExpressions {

  private def getSparkSession(runEnvironment: String): SparkSession = {
    println(s"Creating SparkSession for $runEnvironment")
    val spark = SparkSession.builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }


  def main(args: Array[String]): Unit = {

    val spark = getSparkSession("hml")
    import spark.implicits._
    val df = spark.read
      .option("inferSchema", "true")
      .json(filePopulationJSON)

    // Using Spark Functions and Expressions
    val curr_timestamp_fuct = current_timestamp().cast(StringType).as("timestamp_col_funct")
    val curr_timestamp_expr = expr("cast(current_timestamp() as string) as timestamp_col_expr")
    df.select(curr_timestamp_fuct, curr_timestamp_expr).show(truncate = false)
    df.selectExpr("current_timestamp() as timestamp_col_expr").show(truncate = false)
    // Using Spark SQL
    df.createOrReplaceTempView("cidades")
    spark.sql("select current_timestamp() as timestamp_col_expr from cidades").show(truncate = false)
  }




  /********************************************************************************************************************/
//
//  private def getSparkConfigs(runEnvironment: String) = {
//
//    val conf = Settings.source
//  }

}

