package com.alibaba.graphscope.interactive

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor

import java.util
import scala.beans.BeanProperty

class DataSource() {
  @BeanProperty var scheme : String = "";
  @BeanProperty var location : String = ""; //root location.
};

class InputFormat(){
  @BeanProperty var `type` : String = "";
  @BeanProperty var metadata = new util.HashMap[String,String];
}

class LoadingConfig() {
  @BeanProperty var data_source = new DataSource;
  @BeanProperty var import_option : String = "";
  @BeanProperty var format = new InputFormat;
}

class Column() {
  @BeanProperty var index : Integer = 0;
  @BeanProperty var name : String = "";
}

class ColumnMapping() {
  @BeanProperty var column = new Column;
  @BeanProperty var property : String = "";
}

class VertexMapping() {
  @BeanProperty var type_name : String = "";
  @BeanProperty var inputs = new util.ArrayList[String];
  @BeanProperty var column_mappings = new util.ArrayList[ColumnMapping]
};

class TypeTriplet() {
  @BeanProperty var edge : String = "";
  @BeanProperty var source_vertex : String = "";
  @BeanProperty var destination_vertex : String = "";

  override def toString: String = {
    return "TypeTriplet{" + edge + ", src=" + source_vertex + ", dst=" + destination_vertex;
  }
}

class EdgeMapping() {
  @BeanProperty var type_triplet = new TypeTriplet;
  @BeanProperty var inputs = new util.ArrayList[String];
  @BeanProperty var source_vertex_mappings = new util.ArrayList[ColumnMapping];
  @BeanProperty var destination_vertex_mappings = new util.ArrayList[ColumnMapping];
  @BeanProperty var column_mappings = new util.ArrayList[ColumnMapping];
}

class BulkLoadConfig() {
  @BeanProperty var graph : String = "";
  @BeanProperty var loading_config = new LoadingConfig();
  @BeanProperty var vertex_mappings = new util.ArrayList[VertexMapping];
  @BeanProperty var edge_mappings = new util.ArrayList[EdgeMapping];


  def getVertexColumnMappings(labelName : String) : util.ArrayList[ColumnMapping] = {
    vertex_mappings.forEach(vertex_maping => {
      if (vertex_maping.type_name.equals(labelName)){
        return vertex_maping.column_mappings
      }
    })
    null
  }

  def getEdgeColumnMappings(labelName : String, srcLabel : String, dstLabel : String) : util.ArrayList[ColumnMapping] = {
    edge_mappings.forEach(edge_mapping => {
      val triplet = edge_mapping.type_triplet
      if (triplet.edge.equals(labelName) && triplet.source_vertex.equals(srcLabel) && triplet.destination_vertex.equals(dstLabel)){
        return edge_mapping.column_mappings
      }
    })
    null
  }
}

object BulkLoadConfig {
  def LoadFromYaml(yamlPath : String, spark : SparkSession): BulkLoadConfig = {
    val path = new Path(yamlPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(
      new Constructor(classOf[BulkLoadConfig], new LoaderOptions())
    )
    yaml.load(input).asInstanceOf[BulkLoadConfig]
  }
}
