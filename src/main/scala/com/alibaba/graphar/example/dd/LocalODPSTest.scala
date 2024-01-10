package com.alibaba.graphar.example.dd

import org.apache.spark.sql.SparkSession

object LocalODPSTest {
  def main(args: Array[String]): Unit = {

    val odpsAccessId: String = System.getenv("ODPS_ACCESS_ID")
    val odpsAccessKey: String = System.getenv("ODPS_ACCESS_KEY")
    val odpsProjectName: String = System.getenv("ODPS_PROJECT_NAME")
    val odpsEndPoint: String = System.getenv("ODPS_ENDPOINT")
    if (odpsAccessId == null || odpsAccessKey == null || odpsProjectName == null || odpsEndPoint == null) {
      println("Please set environment variables: ODPS_ACCESS_ID, ODPS_ACCESS_KEY, ODPS_PROJECT_NAME, ODPS_ENDPOINT")
      System.exit(1)
    }
    println("ODPS_ACCESS_ID: " + odpsAccessId)
    println("ODPS_ACCESS_KEY: " + odpsAccessKey)
    println("ODPS_PROJECT_NAME: " + odpsProjectName)
    println("ODPS_ENDPOINT: " + odpsEndPoint)

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkSQL_Demo")
      .config("spark.hadoop.odps.project.name", odpsProjectName)
      .config("spark.hadoop.odps.access.id", odpsAccessId)
      .config("spark.hadoop.odps.access.key", odpsAccessKey)
      .config("spark.hadoop.odps.end.point", odpsEndPoint)
      .config("spark.sql.catalogImplementation", "odps")
      .enableHiveSupport()
      .getOrCreate()

    val res = spark.sql("desc user_node;")
    println(res)
  }
}
