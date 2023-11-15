package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesCalculations extends App {
  val spark = SparkSession.builder()
    .appName("SalesCalculations")
    .master("local[1]")
    .getOrCreate()

  val sellersDF = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/SalesCalculations/Sellers.csv")

  val productsDF = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/SalesCalculations/Products.csv")

  val salesDF = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/SalesCalculations/Sales.csv")

  sellersDF.show(false)
  productsDF.show(false)
  salesDF.show(false)

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------

  println("No. of Sellers : " + sellersDF.count)
  println("No. of Products : " + productsDF.count)
  println("No. of Orders : " + salesDF.count)

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------

  salesDF.agg(countDistinct("product_id")).show(false)

  salesDF.groupBy("product_id")
    .agg(count("*").as("count"))
    .orderBy(col("count").desc)
    .limit(1)
    .show(false)

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------

  salesDF.groupBy("date").agg(countDistinct("product_id").as("ItemSold")).show(false)

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------

  salesDF.join(productsDF, "product_id")
    .agg(sum("price").as("Average_revenue"))
    .select("Average_revenue")
    .show(false)

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------



  //--------------------------------------------------------------------------------------------------------------------------------------------------------------



  //--------------------------------------------------------------------------------------------------------------------------------------------------------------


}
