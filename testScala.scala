package com.datamodeling.sparksql

import org.apache.spark.{SparkConf, SparkContext}


object testScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    println(sc)
    println("it's working....")
  }

}
