package com.knoldus

import io.delta.sharing.client
import org.apache.spark.sql.SparkSession

object ReadSharedData extends App {

  val spark = SparkSession.builder()
    .appName("Open Delta Sharing")
    .master("local[*]")
    .getOrCreate()

  val profilePath = "/home/knoldus/IdeaProjects/Open_Delta_Sharing/src/main/resources/config.share"
  val sharedFiles = client.DeltaSharingRestClient(profilePath).listAllTables()
  sharedFiles.foreach(println)

  val popular_products_df = spark.read
    .format("deltaSharing")
    .load(s"${profilePath}#checkout_data_products.data_products.popular_products_data")
    .limit(20)

  popular_products_df.show()
}