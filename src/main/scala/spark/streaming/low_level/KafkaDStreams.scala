package org.dadaia.spark.streaming.streaming2LowLevel

import Utils.Utils.getSparkSession
import config.Settings.{kafkaCluster, outputClientParquet, topicBronzeClients}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDStreams {

  val spark: SparkSession = getSparkSession("FilesToKafka DStreams")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> kafkaCluster,
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  def readFromKafka() = {
    val topics = Array(topicBronzeClients)
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )
    val processedStream = kafkaDStream.map(record => (record.offset, record.value))
    processedStream.print()

//    ssc.start()
//    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.textFileStream(outputClientParquet)
    val processedData = inputData.map(line => (line.toUpperCase()))
    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val kafkaHasMap = new java.util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHasMap.put(pair._1, pair._2)
        }
        val producer = new KafkaProducer[String, String](kafkaHasMap)
        partition.foreach { value =>
          val msg = new ProducerRecord[String, String](topicBronzeClients, null, value)
          producer.send(msg)
        }
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }




  def main(args: Array[String]): Unit = {
    writeToKafka()
  }
}
