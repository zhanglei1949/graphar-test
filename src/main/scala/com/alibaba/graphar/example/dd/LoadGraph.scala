package com.alibaba.graphar.example.dd

import com.alibaba.graphar.graph.GraphWriter
import com.alibaba.graphscope.interactive.{BulkLoader, BulkLoadConfig, Schema}
import org.apache.spark.sql.SparkSession

class LoadGraph {

  def main(args: Array[String]): Unit = {
    if (args.size != 3){
      System.err.println("Expect 2 args, [schema.yaml] [import.yaml] [output path]")
      System.exit(-1);
    }
    val schemaPath = args(0);
    val importPath = args(1);
    val outputPath = args(2);
    System.out.println("schemaPath "+ schemaPath + ",import path: " + importPath)
    val spark = SparkSession
      .builder()
      .appName("SparkSQL_Demo")
      .enableHiveSupport()
      .getOrCreate()

    val schema = Schema.LoadFromYaml(schemaPath, spark)
    val loadingConfig = BulkLoadConfig.LoadFromYaml(importPath, spark)

    val bulkLoader = new BulkLoader(spark, schema, loadingConfig)
    bulkLoader.loadGraph(outputPath)
  }

//  test("write graphs with data frames") {
//    // initialize a graph writer
//    val writer = new GraphWriter()
//
//    // put the vertex data and edge data into writer
//    val vertex_file_path = getClass.getClassLoader
//      .getResource("gar-test/ldbc_sample/person_0_0.csv")
//      .getPath
//    val vertex_df = spark.read
//      .option("delimiter", "|")
//      .option("header", "true")
//      .csv(vertex_file_path)
//    val label = "person"
//    writer.PutVertexData(label, vertex_df, "id")
//
//    val file_path = getClass.getClassLoader
//      .getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv")
//      .getPath
//    val edge_df = spark.read
//      .option("delimiter", "|")
//      .option("header", "true")
//      .csv(file_path)
//    val tag = ("person", "knows", "person")
//    writer.PutEdgeData(tag, edge_df)
//
//    // conduct writing
//    writer.write("/tmp/ldbc", spark, "ldbc")
//  }
}
