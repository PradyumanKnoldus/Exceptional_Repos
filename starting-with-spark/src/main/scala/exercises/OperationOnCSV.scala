package com.knoldus
package exercises

import org.apache.spark.sql.SparkSession

object OperationOnCSV extends App {

  val spark = SparkSession.builder()
    .appName("averageAge")
    .master("local[1]")
    .getOrCreate()

  val data = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/data.csv")

  println(data)
}
