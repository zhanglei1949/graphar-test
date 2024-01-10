package com.alibaba.graphar.example.dd

import org.apache.spark.sql.SparkSession

object LocalOSSTest {
  def main(args: Array[String]) {
    val ossAccessId: String = System.getenv("OSS_ACCESS_ID")
    val ossAccessKey: String = System.getenv("OSS_ACCESS_KEY")
    val ossEndpoint: String = System.getenv("OSS_ENDPOINT")
    if (ossAccessId == null || ossAccessKey == null || ossEndpoint == null) {
      println("Please set environment variables: OSS_ACCESS_ID, OSS_ACCESS_KEY, OSS_ENDPOINT")
      System.exit(1)
    }
    println("OSS_ACCESS_ID: " + ossAccessId)
    println("OSS_ACCESS_KEY: " + ossAccessKey)
    println("OSS_ENDPOINT: " + ossEndpoint)

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSQL_Demo")
      .config("spark.hadoop.fs.oss.accessKeyId", ossAccessId)
      .config("spark.hadoop.fs.oss.accessKeySecret", ossAccessKey)
      .config("spark.hadoop.fs.oss.endpoint", ossEndpoint)
      .config("spark.hadoop.fs.AbstractFileSystem.oss.impl", "com.aliyun.emr.fs.oss.OSS")
//      .config("spark.hadoop.fs.oss.impl", "com.aliyun.emr.fs.oss.JindoOssFileSystem")
      .config("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    try {
      val pathIn = "oss://zl-transfer/dd/graph_subset.yaml"
      val inputData = sc.textFile(pathIn, 1)
      val cnt = inputData.count
      println(s"count: $cnt")
    } finally {
      sc.stop()
    }
  }
}
