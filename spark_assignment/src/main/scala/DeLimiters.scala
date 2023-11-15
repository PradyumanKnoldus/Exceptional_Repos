package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DeLimiters extends App {
  val spark = SparkSession.builder()
    .appName("DeLimiter")
    .master("local[1]")
    .getOrCreate()

  val data = Seq(
    ("5000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^"))

  import spark.implicits._

  val dataFrame = data.toDF("VALUES", "Delimiter")
  dataFrame.show(false)

  //--------------------------------------------------------------------------------------------------
  // Question 1

  val patternRegex = dataFrame.select($"Delimiter").collect.map(_.getString(0)).mkString(",").replaceAll(",","|")

  dataFrame.withColumn("split_values", split($"VALUES", s"[$patternRegex]")).show(false)

  //--------------------------------------------------------------------------------------------------
  // Question 2

  val wordSplitter = udf((values: String, delimiter: String) => values.split(delimiter(0)))

  dataFrame.select($"VALUES", $"Delimiter", wordSplitter($"VALUES", $"Delimiter").as("split_values")).show(false)

}
