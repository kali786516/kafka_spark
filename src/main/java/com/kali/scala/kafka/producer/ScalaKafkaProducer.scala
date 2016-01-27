package com.kali.scala.kafka.producer

/**
 * Created by kalit_000 on 27/01/2016.
 */

import java.sql.{Connection, DriverManager}
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaKafkaProducer {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SQL_Server_Kafka_Producer").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val url = "jdbc:sqlserver://localhost;user=admin;password=oracle;database=AdventureWorks2014"
    val username = "admin"
    val password = "oracle"

    var connection: Connection = null
    Class.forName(driver)


    /*Create connection and statement to run against sql server and execute*/
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select top 10 CustomerID,StoreID,TerritoryID,AccountNumber from AdventureWorks2014.dbo.Customer")
    resultSet.setFetchSize(10);
    val columnnumber = resultSet.getMetaData().getColumnCount.toInt

    /*OP COLUMN NAMES*/
    var i = 0.toInt;
    for (i <- 1 to columnnumber.toInt)
    {
      val columnname=resultSet.getMetaData().getColumnName(i)
      println("Column Names are:- %s".format(columnname))
    }

    /*OP DATA*/

    while (resultSet.next())
    {
      var list = new java.util.ArrayList[String]()
      for (i <- 1 to columnnumber.toInt)
      {
        list.add(resultSet.getObject(i).toString())
        //println("Column Names are:- %s".format(columnname))
      }
      println(list)

      /*Builds kafka properties file*/
      val props:Properties = new Properties()
      props.put("metadata.broker.list", "localhost:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      /*send message using kafka producer.send to topic trade*/
      val config= new ProducerConfig(props)
      val producer= new Producer[String,String](config)
      //val x=list.collect().mkString("\n").replace("[","").replace("]","").replace(",","~")
      producer.send(new KeyedMessage[String, String]("first", list.toString().replace("[","").replace("]","").replace(",","~").replace(" ","")))


    }

    /*close SQL Server database connection*/
    connection.close()


  }

}
