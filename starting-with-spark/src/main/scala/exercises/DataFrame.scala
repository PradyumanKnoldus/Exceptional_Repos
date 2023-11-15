/*
  -> Write a Spark code snippet to calculate the total count of records in a given DataFrame.
  -> Given a DataFrame with columns "name" and "age", write Spark code to filter out all the records where the age is greater than 30.
  -> Write a Spark program to calculate the average value of a specific numeric column in a DataFrame.
  -> Given a DataFrame containing sales data with columns "product" and "quantity", write Spark code to find the product with the highest quantity sold.
  -> Write a Spark program to join two DataFrames based on a common column and display the combined result.
  -> Given a DataFrame with columns "date" and "revenue", write Spark code to calculate the total revenue earned for each month.
  -> Write a Spark program to load a JSON file and extract specific fields to create a new DataFrame.
  -> Given a DataFrame containing customer information with columns "name", "age", and "country", write Spark code to find the most common country among the customers.
  -> Write a Spark program to read data from a CSV file and convert it into Parquet format.
  -> Given a DataFrame with columns "product", "price", and "quantity", write Spark code to calculate the total sales value (price * quantity) for each product.
*/

package com.knoldus
package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame extends App {

  val spark = SparkSession.builder()
    .appName("DataFramesPractice")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/dataframeInfo.csv")

  df.show(false)

  // QUESTION 1
  df.select(count("name").as("Total Count")).show(false)
  println(s"Total No. of Records : ${df.count()}")

  // QUESTION 2
  df.where("age > 30").show(false)
  df.filter("age > 30").show(false)

  //QUESTION 3
  df.agg(avg(col("age")).as("Average Age")).show(false)
  df.agg(avg(col("quantity")).as("Average Age")).show(false)

  //QUESTION 4
  df.groupBy("product").agg(sum(col("quantity")).as("total_sales")).orderBy(col("total_sales").desc).limit(1).show(false)

  //QUESTION 5
  import spark.implicits._

  val data = Seq(
    ("John", 3),
    ("Alice", 2),
    ("Bob", 5),
    ("Charlie", 0),
    ("Emily", 1),
    ("David", 3),
    ("Emma", 3),
    ("Oliver", 2)
  )

  val schema = Seq("name", "experience")

  val df2 = data.toDF(schema: _*)

  df2.show(false)

  df.join(df2, "name").show(false)

  //QUESTION 6
  val dataSeq = Seq(
    ("2023-01-01", 1000.0),
    ("2023-01-15", 1200.0),
    ("2023-02-03", 800.0),
    ("2023-02-22", 1500.0),
    ("2023-03-10", 2000.0),
    ("2023-03-18", 1800.0)
  )

  val schema1 = Seq("date", "revenue")

  val df3 = dataSeq.toDF(schema1: _*)

  df3.withColumn("month", month(col("date")))
    .groupBy("month").agg(sum("revenue")).as("Total Revenue").show(false)

  // QUESTION 7
  val df4 = spark.read.options(Map("multiline" -> "true")).json("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/json/data.json")
  df4.show(false)

  // QUESTION 8
  df4.groupBy("country").agg(count("country").as("country_count")).orderBy(col("country_count").desc).limit(1).show(false)

  // QUESTION 9
//  df.write.format("parquet").save("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/in_parquet_format")

  // QUESTIONS 10
  val df5 = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/sales_dat.csv")

  df5.show(false)

  df5.withColumn("Total Sales", $"price" * $"quantity").groupBy("product").agg(sum("Total Sales").as("Total Sales Value")).show(false)

//  df.cache()
//  df.persist()

  println(df.first.getAs[Int]("age"))

  df.sample(true, 0.50).show(false)

}



