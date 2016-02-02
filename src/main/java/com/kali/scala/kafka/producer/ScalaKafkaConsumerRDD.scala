package com.kali.scala.kafka.producer

/**
 * Created by kalit_000 on 02/02/2016.
 */

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder

object ScalaKafkaConsumerRDD {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Scala_Kafka_Consumer_DF").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val zkQuorm="localhost:2181"
    val group="test-group2"
    val topic=Set("first")
    val numThreads=1
    val broker="localhost:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)

    // change these values to offsets that actually exist for the topic
    val offsetRanges = Array(
        OffsetRange(topic.toSet.head,0,200,349)
      )


    val rdd= KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, offsetRanges)
    rdd.collect.foreach(println)
    sc.stop()

  }
}
