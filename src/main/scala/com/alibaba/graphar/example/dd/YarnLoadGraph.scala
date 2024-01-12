package com.alibaba.graphar.example.dd

import com.alibaba.graphscope.interactive.{BulkLoadConfig, BulkLoader, Schema}
import org.apache.hadoop.fs
import org.apache.spark.sql.SparkSession

/*
  Run in yarn cluster mode.
 */
object YarnLoadGraph {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.hadoop.fs.AbstractFileSystem.oss.impl", "com.aliyun.emr.fs.oss.OSS")
      .config("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      .enableHiveSupport()
      .getOrCreate()

    val schema_path = new fs.Path("oss://zl-transfer/dd/graph.yaml")
    val fileSystem = fs.FileSystem.get(schema_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val schema_input = fileSystem.open(schema_path)
    val schema = Schema.LoadFromYaml(schema_input, spark)

    val config_path = new fs.Path("oss://zl-transfer/dd/full_import.yaml")
    val fileSystem2 = fs.FileSystem.get(config_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val config_input = fileSystem2.open(config_path)
    val config = BulkLoadConfig.LoadFromYaml(config_input, spark)

    var loader = new BulkLoader(spark, schema, config)
    assert(loader.loadGraph("oss://zl-transfer/dd/output3"))
  }
}
