package com.datamodeling.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.cassandra._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}


object cassandra_spark_sql {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local")
      .appName("firstapp")
      .getOrCreate()

    val df = spark.read.cassandraFormat("books_by_author", "my_status").load
    df.explain()
    df.printSchema()

    df.createOrReplaceTempView("bookstbl")

    val dftbl = spark.sql("select * from  bookstbl").show()

    val booksdf = df.toDF()

  }
}
