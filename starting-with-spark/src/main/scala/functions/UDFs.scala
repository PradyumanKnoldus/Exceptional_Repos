package com.knoldus
package functions

import org.apache.spark.sql.SparkSession

object UDFs extends App {
  val spark = SparkSession.builder()
    .appName("builtInFunctions")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._


}
