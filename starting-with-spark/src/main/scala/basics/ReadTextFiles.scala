package com.knoldus
package basics

import org.apache.spark.sql.SparkSession

object ReadTextFiles extends App {

  val spark = SparkSession.builder()
    .appName("sparkSession")
    .master("local[1]")
    .getOrCreate()

  val rdd = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/text/*")
  rdd.foreach(f => {
    println(f)
  })

  val rddWhole = spark.sparkContext.wholeTextFiles("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/text/*")
  rddWhole.foreach(f => {
    println(s"${f._1} => ${f._2}")
  })
}
