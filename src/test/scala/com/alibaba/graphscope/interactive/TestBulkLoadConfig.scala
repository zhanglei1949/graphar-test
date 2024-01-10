package com.alibaba.graphscope.interactive

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TestBulkLoadConfig  extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("Test load graph via spark to a temporary path") {
    val yaml_path = getClass.getClassLoader
      .getResource("modern_graph/import.yaml")
      .getPath
    val bulkLoadConfig = BulkLoadConfig.LoadFromYaml(yaml_path,spark)
    assert(bulkLoadConfig.graph == "modern_graph")
    assert(bulkLoadConfig.loading_config.format.`type` == "csv");
  }
}
