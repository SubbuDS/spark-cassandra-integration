package com.datamodeling.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

object spark_structured_Streaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("spark structured streaming")
      .getOrCreate()

    val streamingInputDF = spark.readStream
                                .format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("subscribe","sampleJson")
                                .option("startingOffsets", "earliest")
                                .load()

    val personStringDF = streamingInputDF.selectExpr("CAST(value AS STRING)")


    val schema = new StructType()
      .add("cust_id",IntegerType)
      .add("month",IntegerType)
      .add("expenses",IntegerType)

    val topicSchema_DF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")




  }

}
