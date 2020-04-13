package com.datamodeling.sparksql

import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object spark_kafka_integration {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("spak kafka integration").getOrCreate()

    val ds1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") .option("subscribe", "USERS1")
      .load()

    ds1.selectExpr("USERS1", "CAST(key AS STRING)", "CAST(value AS STRING)")
       .writeStream.format("kafka")
     // .option("checkpointLocation", "/to/HDFS-compatible/dir")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .start()

    print("spark integration with kafka")
  }

}
