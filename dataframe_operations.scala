package com.datamodeling.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}


object dataframe_operations {

  def main(args: Array[String]): Unit = {

    // creating sparksession object
    val spark = SparkSession.builder()
                            .master("local")
                            .appName("firstapp")
                            .getOrCreate()

    // Reading csv file and creating a dataframe using sparksession object
    val df = spark.read
                //.format("csv")
                  .option("header", "true").csv("/Users/iphonec/Downloads/books.csv")
        df.show()
        df.printSchema()


    /*
    // Rename a cloumn from dataframe
    df.withColumnRenamed("# num_pages","num_pages").show(10)

    df.withColumn("CopiedColumn",df("salary")* -1)
    .show(false)  */

    // create a temp view from dataframe
    println("temp table books_tbl has been created")
        df.createOrReplaceTempView("books_tbl")

    println("printing data from books table SQL like query ")
    val books_tableDF = spark.sql("SELECT  title, bookID,authors FROM books_tbl").show()


    // creating a dataframe from List of objects
    val df2 = spark.createDataFrame(List(("subbu", 151),("user1", 63)))

    val nameddf = df2.toDF("UserName","Score").show()

   // select columns from table  using sql staement
    val nameDF = spark.sql("select authors from books_tbl").show()

    //select columns from dataframe
    val selectcols = df.select("authors","bookID","isbn").show(5)


    //create an RDD
    val namerdd = spark.sparkContext.makeRDD(Array(4,5,6,7,8,9,10))


  }

}
