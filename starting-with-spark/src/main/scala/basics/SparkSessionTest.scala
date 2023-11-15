package com.knoldus
package basics

import org.apache.spark.sql.SparkSession

object SparkSessionTest extends App {

  // Creating SparkSession
  val spark = SparkSession.builder()
                          .master("local[1]")
                          .appName("FirstSparkSession")
                          .config("spark.some.config.option", "config-value")
//                          .enableHiveSupport()
                          .getOrCreate()

  //--------------------------------------------------------------------------------------

  println(s"Spark Version: ${spark.version}")
  println(spark)

  val spark2 = SparkSession.builder().getOrCreate()
  println(spark2)

  val spark3 = spark.newSession()
  println(spark3)

  //--------------------------------------------------------------------------------------

  // Set & Get All Spark Config
  spark.conf.set("spark.sql.shuffle.partitions", "30")
  val configMap: Map[String, String] = spark.conf.getAll

  //--------------------------------------------------------------------------------------

  // Creating DataFrame
  val df = spark.createDataFrame(
    List(("Scala", 15), ("Java", 6), ("Kotlin", 3), ("DevOps", 16), ("Automation", 3), ("AI", 2))
  )

  df.show()

  //--------------------------------------------------------------------------------------

  // Spark SQL
  df.createOrReplaceTempView("sample_table")

  val df2 = spark.sql("SELECT * FROM sample_table")
  df2.show()

  //--------------------------------------------------------------------------------------

//  // Create Hive Table
//  spark.table("sample_table").write.saveAsTable("sample_hive_table")
//  val df3 = spark.sql("SELECT * FROM sample_table")
//  df3.show()

  //--------------------------------------------------------------------------------------

  // Working with Catalogs (DataSet)
  val ds = spark.catalog.listDatabases()
  ds.show(false)

  val ds2 = spark.catalog.listTables()
  ds2.show(false)

  //--------------------------------------------------------------------------------------

  // SparkContext
  println(spark.sparkContext)
  println(s"Spark App Name: ${spark.sparkContext.appName}")

  //--------------------------------------------------------------------------------------

  // Create RDD
  val rdd = spark.sparkContext.range(1,5)
  rdd.collect().foreach(print)

  val rdd2 = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/textFile1.txt")

  //--------------------------------------------------------------------------------------

  // SparkContext Stop
  spark.sparkContext.stop()


}

