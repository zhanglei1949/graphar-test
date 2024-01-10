package com.alibaba.graphar.example.dd


import com.alibaba.graphscope.interactive.{BulkLoadConfig, BulkLoader, Schema}
import org.apache.hadoop.fs
import org.apache.spark.sql.SparkSession

object LocalModeLoadGraph {
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
      .config("spark.hadoop.odps.project.name", odpsProjectName)
      .config("spark.hadoop.odps.access.id", odpsAccessId)
      .config("spark.hadoop.odps.access.key", odpsAccessKey)
      .config("spark.hadoop.odps.end.point", odpsEndPoint)
      .config("spark.sql.catalogImplementation", "odps")
      .config("spark.hadoop.fs.oss.accessKeyId", ossAccessId)
      .config("spark.hadoop.fs.oss.accessKeySecret", ossAccessKey)
      .config("spark.hadoop.fs.oss.endpoint", ossEndpoint)
      .config("spark.hadoop.fs.AbstractFileSystem.oss.impl", "com.aliyun.emr.fs.oss.OSS")
      .config("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      .enableHiveSupport()
      .getOrCreate()

    val conf = spark.conf
    println("accessId {}", conf.get("spark.hadoop.fs.oss.accessKeyId"))
    println("accessKey {}", conf.get("spark.hadoop.fs.oss.accessKeySecret"))
    println("endpoint {}", conf.get("spark.hadoop.fs.oss.endpoint"))

    val schema_path = new fs.Path("oss://zl-transfer/dd/graph_subset.yaml")
    val fileSystem = fs.FileSystem.get(schema_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val schema_input = fileSystem.open(schema_path)
    val schema = Schema.LoadFromYaml(schema_input, spark)

    val config_path = new fs.Path("oss://zl-transfer/dd/import_subset.yaml")
    val fileSystem2 = fs.FileSystem.get(config_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val config_input = fileSystem2.open(config_path)
    val config = BulkLoadConfig.LoadFromYaml(config_input, spark)

    var loader = new BulkLoader(spark, schema, config)
    assert(loader.loadGraph("oss://zl-transfer/dd/output"))
  }

}
