package com.knoldus
package basics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("sparkSession")
    .master("local[1]")
    .getOrCreate()

  // Create RDD
  val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
  val rddCollect: Array[Int] = rdd.collect()

  println(s"Number of Partitions: ${rdd.getNumPartitions}")
  println(s"Action: First element: ${rdd.first()}")
  println("Action: RDD converted to Array[Int] : ")
  rddCollect.foreach(println)

  // ---------------------------------------------------------------------------

  // Create Empty RDD
  spark.sparkContext.parallelize(Seq.empty[String])     // One Way
  spark.sparkContext.emptyRDD                           // Another Way
}
