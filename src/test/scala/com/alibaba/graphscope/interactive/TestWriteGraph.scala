package com.alibaba.graphscope.interactive

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory


class TestWriteGraph extends AnyFunSuite{
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("load bulk load config from yaml") {
    val config_yaml_path = getClass.getClassLoader
      .getResource("modern_graph/import.yaml")
      .getPath
    val bulkLoadConfig = BulkLoadConfig.LoadFromYaml(config_yaml_path,spark)
    val schema_yaml_path = getClass.getClassLoader
      .getResource("modern_graph/graph.yaml")
      .getPath
    val schema = Schema.LoadFromYaml(schema_yaml_path,spark)

    System.out.println(schema.schema.edge_types)

    val directory = new Directory(new File("/tmp/graphar_modern_graph"))
    directory.deleteRecursively()

    var loader = new BulkLoader(spark, schema, bulkLoadConfig)
    assert(loader.loadGraph("/tmp/graphar_modern_graph"))
  }
}
