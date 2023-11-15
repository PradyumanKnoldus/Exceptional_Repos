package com.knoldus
package exercises

import org.apache.spark.sql.SparkSession

object WordCounter extends App {
  val spark = SparkSession.builder()
    .appName("WorkCount")
    .master("local[1]")
    .getOrCreate()

  val textFileRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/text/some_random.txt")

  val splitRDD = textFileRDD.flatMap(line => line.split("\\s"))

  val filterOutSpecialChars = splitRDD.map(word => word.replaceAll("[^a-zA-Z0-9\\s:]", "").toLowerCase)

  val mappingWords = filterOutSpecialChars.map(word => (word, 1))

  val reducingWords = mappingWords.reduceByKey(_ + _).sortByKey()

  reducingWords.foreach { case (word, count) =>
    println(s"$word : $count")
  }

  reducingWords.map { case (word, count) =>
    s"$word: $count"
  }.saveAsTextFile("/home/knoldus/IdeaProjects/starting-with-spark/src/main/resources/text/output_File.txt")
}
