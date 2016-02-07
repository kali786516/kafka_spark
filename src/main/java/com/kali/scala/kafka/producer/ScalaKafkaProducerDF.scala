package com.kali.scala.kafka.producer

/**
 * Created by kalit_000 on 27/01/2016.
 */

import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.support.ClassPathXmlApplicationContext
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig

case class SourceDB(driver:String,url:String,username:String,password:String,table:String,lowerbound:String,upperbound:String,numberofparitions:String,parallelizecolumn:String)

object ScalaKafkaProducerDF {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Scala_Kafka_Prodcuer_DF").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    //read the application context file
    val ctx = new ClassPathXmlApplicationContext("producerconfig.xml")
    val DBinfo = ctx.getBean("producer").asInstanceOf[SourceDB]

    println("DB Driver:-%s".format(DBinfo.driver))
    println("DB Url:-%s".format(DBinfo.url))
    println("Username:-%s".format(DBinfo.username))
    println("Password:-%s".format(DBinfo.password))
    println("Table:-%s".format(DBinfo.table))
    println("Lowerbound:-%s".format(DBinfo.lowerbound.toInt))
    println("Upperbound:-%s".format(DBinfo.upperbound.toInt))
    println("Numberofpartitions:-%s".format(DBinfo.numberofparitions.toInt))
    println("Parallelizecolumn:-%s".format(DBinfo.parallelizecolumn))

    try {

      val props=new Properties()
      props.put("user",DBinfo.username)
      props.put("password",DBinfo.password)
      props.put("driver",DBinfo.driver)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val df = sqlContext.read.jdbc(DBinfo.url,DBinfo.table,DBinfo.parallelizecolumn,DBinfo.lowerbound.toInt,DBinfo.upperbound.toInt,DBinfo.numberofparitions.toInt,props)
      df.show(10)

      /*Builds kafka properties file*/
      val kafkaProps = new Properties()
      kafkaProps.put("metadata.broker.list", "localhost:9092")
      kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder")

      /*send message using kafka producer.send to topic first*/
      val config= new ProducerConfig(kafkaProps)
      val producer= new Producer[String,String](config)


      for (test <- df.collectAsList().toArray)
      {
        producer.send(new KeyedMessage[String, String]("first", test.toString.replace("[","").replace("]","").replace(",","~").replace(" ","")))
        producer.close()
      }


      } catch {
      case e: Exception => e.printStackTrace
    }
    sc.stop()
  }
}
