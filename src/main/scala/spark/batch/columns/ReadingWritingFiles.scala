package spark.batch.columns

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StringType

object ReadingWritingFiles extends App {
  val spark = SparkSession.builder()
    .appName("Spark Essentials")
    .config("spark.master", "local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  // Set the log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  private val path_input = "data/csv/cidades.csv"

  private val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(path_input)

  val column_1: Column = df("id") // Apply Method of object Dataframe -> Column
  val column_2: Column = df.col("city") // Apply Method of object Dataframe -> Column
  val column_3: Column = expr("country") // Apply Method of object Dataframe -> Column
  val column_4: Column = col("area") // Apply Method of object Dataframe -> Column

  val df_1 = df.select("id", "city", "country", "area")
  val df_2 = df.select(df("id"), col("city"), $"country", expr("area"))
  val df_3 = df.select(column_1, column_2, column_3, column_4)

  val new_column = column_1 + 1
  val new_column_2 = column_1 + 0.0
  val new_column_3 = column_1.cast(StringType)

  val df_final =
    df.withColumn("id_p1", new_column)
      .withColumn("id_float", new_column_2)
      .withColumn("id_str", new_column_3)

  df_final.select(df_final.columns.map(col): _*)
    .filter(new_column > 2.0)
    .filter(df("area") > 300.0)
    .filter(col("country") === "Brazil")
    .show()
}
