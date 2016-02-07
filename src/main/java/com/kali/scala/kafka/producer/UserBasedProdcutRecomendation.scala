package com.kali.scala.kafka.producer

/**
 * Created by kalit_000 on 07/02/2016.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating


object UserBasedProdcutRecomendation {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("UserProdcutBasedRecommen").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val songs=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\kagle_songs.txt")
    val users=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\kagleusers.txt")
    val triplets=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\kaggle_visible_evaluation_triplets.txt")

    val songIndex=songs.map(_.split("\\W+")).map(x => (x(0),x(1).toInt))
    val songMap=songIndex.collectAsMap()

    val userIndex=users.zipWithIndex.map(x => (x._1,x._2.toInt))
    val userMap=userIndex.collectAsMap()

    //userIndex.take(10).foreach(println)
    //songIndex.take(10).foreach(println)

    val broadcastSongMap=sc.broadcast(songMap)
    val broadcastUserMap=sc.broadcast(userMap)


    val tripArray=triplets.map(_.split("\\W+"))

    val rating=tripArray.map { case Array(user,song,plays) =>
    val userId=broadcastUserMap.value.getOrElse(user,0)
    val songId=broadcastSongMap.value.getOrElse(song,0)
        Rating(userId,songId,plays.toDouble)
    }

    val model=ALS.trainImplicit(rating,10,10)
    val usersongs=rating.map(r => (r.user,r.product))
    val predictions=model.predict(usersongs)


    val userIndex2=users.zipWithIndex.map(x => (x._2.toInt,x._1))
    val userMap2=userIndex2.collectAsMap()
    val broadcastUserMap2=sc.broadcast(userMap2)

    val songIndex2=songs.map(_.split("\\W+")).map(x => (x(1).toInt,x(0)))
    val songMap2=songIndex2.collectAsMap()
    val broadcastSongMap2=sc.broadcast(songMap2)


    rating.map(r => (r.rating,broadcastUserMap2.value(r.user),r.user.toString,broadcastSongMap2.value.getOrElse(r.product,0),r.product)).sortBy(_._1,false).take(5).foreach(println)



  }


}
