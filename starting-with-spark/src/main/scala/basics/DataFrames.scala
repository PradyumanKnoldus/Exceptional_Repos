package com.knoldus
package basics

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrames extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("sparkSession")
    .master("local[1]")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Array(1,2,3,4,5,6))
  val schema = StructType{
    StructField("Numbers", IntegerType, nullable = false) :: Nil
  }

  val rowRDD = rdd.map(line => Row(line))

  val df = spark.sqlContext.createDataFrame(rowRDD, schema)

  df.printSchema()
  df.show()

  //-----------------------------------------------------------------------------

  val df1 = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/csv/data.csv")

  df1.show(false)

}
