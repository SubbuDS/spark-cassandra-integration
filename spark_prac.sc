import org.apache.spark

import scala.util.Random

val a = "stringdata"

print(a)

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.sql.SparkSession

//val spark = SparkSession.builder().master("local(*)")appName("spark practice").getOrCreate()


val spark = SparkSession.builder()
  .master("local")
  .appName("firstapp")
  .getOrCreate()

val peopleRDD = spark.sparkContext.parallelize(Array(Row(1L, "John Doe",  30L),
  Row(2L, "Mary Jane", 25L)))

peopleRDD.count()
peopleRDD.collect()


