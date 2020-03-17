package com.datamodeling.sparksql


import java.util.Date

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
object spark_cassandra_test {

  def main(args: Array[String]): Unit = {

   // val  conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost").setAppName("spark cassandra integration").setMaster("local")


    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost").setMaster("local").setAppName("spark_cassandra integration")

    val  sc = new SparkContext(conf)

    val rdd = sc.cassandraTable("my_status", "books_by_author")

    rdd.foreach(println)
    rdd.select("author_name").foreach(println)

    sc.cassandraTable("my_status","books_by_author")
      .select("author_name","book_name","rating")
      .where("author_name = 'James Patterson' and publish_year > 2008")
      .collect()
      .foreach(println)
   //case class authors(author_name: String, publish_year: Int,book_id: String, book_name:String, book_timeuuid: Date, rating: Float )

    val spark = SparkSession.builder()
      .master("local")
      .appName("firstapp")
      .getOrCreate()

     val df = spark.read.cassandraFormat("books_by_author", "my_status").load
    df.explain()
    df.printSchema()
    df.createOrReplaceTempView("bookstbl")

    val dftbl = spark.sql("select * from  bookstbl").show()


    /*val conf = new SparkConf()
             conf.set("spark.cassandra.connection.host","localhost")
             conf.setMaster("local[*]")
             conf.setAppName("spark cassandra integratin")

    val sc = SparkContext(conf)*/





  }

}
