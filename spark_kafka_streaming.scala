package com.datamodeling.sparksql


import org.apache.spark.sql.SparkSession

object spark_kafka_streaming {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq (("iphone", "2007"),("iphone 3G","2008"),
      ("iphone 3GS","2009"),
      ("iphone 4","2010"),
      ("iphone 4S","2011"),
      ("iphone 5","2012"),
      ("iphone 8","2014"),
      ("iphone 10","2017"))

    val df = spark.createDataFrame(data).toDF("key","value")

    df.show()

    df.write
     .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","text_topic")
      .save()

}

}
