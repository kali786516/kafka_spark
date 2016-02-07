package com.kali.scala.kafka.producer

/**
 * Created by kalit_000 on 07/02/2016.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object SparkUserRecomendation {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark_userid_recomendation").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val data=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\ml-100k\\u.data")

    val ratings=data.map { line =>
    val Array(userID,itemID,rating,_) = line.split("\t")
      Rating(userID.toInt,itemID.toInt,rating.toDouble)
    }

  // ratings.foreach(println)


    //my movie rating data
    val pdata=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\ml-100k\\p.dat")

    val pratings=pdata.map {line =>
      val Array(userID,itemId,rating) = line.split(",")
      Rating(userID.toInt,itemId.toInt,rating.toDouble)
      }

    // join my user rating data with movie rating data, (rank=5,10 iterations,0.01 as lambda)
    val movierating=ratings.union(pratings)
    //movierating.foreach(println)
    val model=ALS.train(movierating,10,10,0.01)


/*
    //recommend movie for users .....
    val test=model.recommendProductsForUsers(944).take(3)

    val moviesfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\movie_recomendation\\ml-100k\\movies.dat")
    val test2=moviesfile.map(x => x.split("::")).map(x => (x(0),x(1)))
    val test3=sc.broadcast(test2.collect().toMap)
    //println(test3.value(1.toString))


    test.foreach { r =>
     println("MovieID %d".format(r._1)+":Movie Name:"+ test3.value(r._1.toString))
    }
*/
    //best product recomendation I think .......
    model.predict(sc.parallelize(Array((944,195)))).collect.foreach(println)
    model.predict(sc.parallelize(Array((944,148)))).collect.foreach(println)

  }


}
