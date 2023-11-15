package com.knoldus
package basics

import org.apache.spark.sql.SparkSession

object ReadCSVFile extends App {

  val spark = SparkSession.builder()
    .appName("sparkSession")
    .master("local[1]")
    .getOrCreate()

  val readedFile = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/csvFile1.csv")
  val rdd = readedFile.map(f => {
    f.split(",")
  })

  rdd.foreach(f => {
    println(s"${f(0)} -> ${f(1)}")
  })
}
