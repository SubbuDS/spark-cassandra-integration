package com.datamodeling.sparksql

import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
//import spark.implicits._
import org.bson.Document
import com.mongodb.spark.sql._
import com.mongodb.spark.config._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.language.implicitConversions
import org.apache.spark.sql.functions.{max, min}

import com.mongodb.casbah.Imports._

import com.mongodb.spark.annotation.DeveloperApi


object spark_mongodb {

  def main(args: Array[String]): Unit = {

    //package com.datamodeling.sparksql

    case class flightinfo (depatureAirport: String,arrivalAirport: String,aircraft: String, distance: Int,intercontinental: Boolean )


    // val spark = SparkSession.builder()
     // .master("local")
      //.appName("spark_mongodb_integration")
      //.getOrCreate()


    val spark = SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .getOrCreate()


    //val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "flights", "collection" -> "flightsData")) // 1)



    val personDf = spark.read.mongo(ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "flights", "collection" -> "flightsData")))

    personDf.printSchema()

    val dftbl = personDf.createOrReplaceTempView("mtbl")

    spark.sql("select * from mtbl").show()

    personDf.show()

    println(personDf)
    //val zipDf = spark.read.mongo(readConfig)

    //zipDf.printSchema() // 3)
    //zipDf.show()





    //val flights_df = spark.read.format("com.mongodb.spark.sql.DefaultSource")
                            //.option("database", "flights")
                          //  .option("collection", "flightsData").load()

   // df.show()


  }

}
