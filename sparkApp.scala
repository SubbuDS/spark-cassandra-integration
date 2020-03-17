package com.datamodeling.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession

object sparkApp {

  def main(args: Array[String]): Unit = {

    // creating SparkConf object  and SparkContext to initialize spark
   val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("spark first application")

    val sc = new SparkContext(conf)

    println("sparkConf and SparkContext has been created....")

    //val spark = SparkSession.builder().master("local").appName("firstapp").getOrCreate()
    val rdd = sc.makeRDD(Array(1,2,3,4,5,6))
    rdd.collect().foreach(println)





  }
}
