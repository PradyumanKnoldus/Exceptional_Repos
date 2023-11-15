package com.knoldus
package basics

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextTest extends App {

  val sparkConf = new SparkConf().setAppName("sparkContextExample").setMaster("local[1]")
  val sc = SparkContext.getOrCreate(sparkConf)
  val rdd = sc.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/textFile1.txt")

  sc.setLogLevel("ERROR")

  println("First SparkContext:")
  println(s"App Name: ${sc.appName}")
  println(s"Deploy Mode: ${sc.deployMode}")
  println(s"Master: ${sc.master}")
//  sc.stop()

  val sparkConf2 = new SparkConf().setAppName("sparkContextExample2").setMaster("local[1]")
  val sc2 = SparkContext.getOrCreate(sparkConf2)

  println("Second SparkContext:")
  println(s"App Name: ${sc2.appName}")
  println(s"Deploy Mode: ${sc2.deployMode}")
  println(s"Master: ${sc2.master}")
}
