package com.alibaba.graphscope.interactive

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TestSchema  extends AnyFunSuite{

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("load schema from yaml") {
    val yaml_path = getClass.getClassLoader
      .getResource("modern_graph/graph.yaml")
      .getPath
    val schema = Schema.LoadFromYaml(yaml_path,spark)
    assert(schema.getName == "modern_graph")
    assert(schema.getSchema.vertex_types.size() == 2);
    assert(schema.getSchema.edge_types.size() == 2);
  }
}
