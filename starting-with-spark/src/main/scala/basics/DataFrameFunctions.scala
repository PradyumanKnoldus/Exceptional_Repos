package com.knoldus
package basics

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameFunctions extends App {
  val spark = SparkSession.builder()
    .appName("Built In Functions")
    .master("local[1]")
    .getOrCreate()

//  val df = spark.read
//    .options(Map("header" -> "true", "inferSchema" -> "true"))
//    .csv("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/dataframeInfo.csv")
//
//  df.show()
//
//  df.toDF()
//
//  println(df.dtypes)
//
//  println(df.isLocal)
//
//  println(df.isEmpty)
//
//  println(df.isStreaming)
//
////  println(df.checkpoint())
////  println(df.checkpoint(true))
//
//  println(df.localCheckpoint())
//  println(df.localCheckpoint(true))
//
//  println(df.na)
//
//  println(df.stat)

//  import spark.implicits._

  val schema = StructType(Seq(
    StructField("Name", StringType, nullable = false),
    StructField("Age", IntegerType, nullable = false),
    StructField("City", StringType, nullable = false)
  ))

  val df = spark.sparkContext.parallelize(Seq(
    ("Alice", 30, "New York"),
    ("Bob", 25, "San Francisco"),
    ("Charlie", 35, "Los Angeles")
  ))

  val df1 = spark.createDataFrame(df.map(line => Row(line._1, line._2, line._3)), schema)
  df1.show(false)

  df1.agg()
}

// Stat
// colRegex
















