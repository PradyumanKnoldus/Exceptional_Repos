package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UpperCase extends App {
  val spark = SparkSession.builder()
    .appName("UpperCase")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    ("0","Warsaw","Poland"),
    ("1","Villeneuve-Loubet","France"),
    ("2","Vranje","Serbia"),
    ("3","Pittsburgh","US")
  ).toDF("id","city","country")

  df.show(false)

  val my_upper = udf((inputString: String) => inputString.toUpperCase())

  df.withColumn("upper_city", my_upper($"city")).show(false)
}
