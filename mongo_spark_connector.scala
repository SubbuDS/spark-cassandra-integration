package com.datamodeling.sparksql

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.sql._

object mongo_spark_connector  {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    //val ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource")
     // .option("database", "flights").option("collection", "flightsData").load()
    //import com.mongodb.spark._

    //ratings.show()
    val personDf = spark.read.mongo(ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "flights", "collection" -> "flightsData")))

    personDf.printSchema()
    val coldf = personDf.toDF("_id","aircraft","arrivalAirport","depatureAirport","distance","intercontinental")

   coldf.show()
  }
}
