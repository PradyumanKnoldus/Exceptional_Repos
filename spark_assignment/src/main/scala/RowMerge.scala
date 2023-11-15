package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RowMerge extends App {
  val spark = SparkSession.builder()
    .appName("RowMerge")
    .master("local[1]")
    .getOrCreate()

  val data = Seq(
    ("100", "John", Some(35), None),
    ("100", "John", None, Some("Georgia")),
    ("101", "Mike", Some(25), None),
    ("101", "Mike", None, Some("New York")),
    ("103", "Mary", Some(22), None),
    ("103", "Mary", None, Some("Texas")),
    ("104", "Smith", Some(25), None),
    ("105","Jake", None,Some("Florida")))

  import spark.implicits._

  val dataFrame = data.toDF("id", "name", "age", "city")
  dataFrame.show(false)

  dataFrame.groupBy("id", "name")
    .agg(
      coalesce(first("age"), last("age")).as("age"),
      coalesce(first("city"), last("city")).as("city")).show(false)

}
