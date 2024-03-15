package org.dadaia.spark.streaming.streaming2LowLevel

import Utils.Utils.getSparkSession
import common.Person
import config.Settings.{inputBasePath, outputBasePath, socketHost, socketOutPort1}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

object DStreamsTransformations {



  val spark: SparkSession = getSparkSession("DStreams transformations")
  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople(): DStream[Person] = ssc.socketTextStream(socketHost, socketOutPort1).map {
    line =>
      val tokens = line.split(":")
      Person(
        tokens(0).toInt,
        tokens(1),
        tokens(2),
        tokens(3),
        tokens(4),
        Date.valueOf(tokens(5)),
        tokens(6),
        tokens(7).toInt
      )
  }

  // 1. Map
  def peopleAges() = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  // 2. Flatmap
  def peopleSmallNames() = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  // 3. Filter
  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  // 4. Counter. PER BATCH
  def countPeople(): DStream[Long] = readPeople().count() // the number of entries in every batch

  // 5. Count by value. PER BATCH
  def countNames(): DStream[(String, Long)] = readPeople().map(person => person.firstName).countByValue()

  // 6. Reduce by key. PER BATCH. WORK WITH TUPLES
  def countNamesReduce(): DStream[(String, Long)] =
    readPeople()
      .map(person => person.firstName)
      .map(name => (name, 1L))
      .reduceByKey((a, b) => a + b)

  def saveToJson() = readPeople().foreachRDD { rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File(s"$outputBasePath/people")
    val nFiles = f.listFiles().length
    val path = s"$outputBasePath/people/people_$nFiles.json"
    ds.write.json(path)
  }

  def main(args: Array[String]): Unit = {
    val stream = highIncomePeople()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
