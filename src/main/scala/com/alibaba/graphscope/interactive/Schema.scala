package com.alibaba.graphscope.interactive

import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.{LoaderOptions, Yaml}

import java.util
import scala.beans.BeanProperty

class PropertyType() {
  @BeanProperty var primitive_type: String = "";
  //TODO: support comprehensive types
};

class Property() {
  @BeanProperty var property_id: Integer = 0;
  @BeanProperty var property_name: String = "";
  @BeanProperty var property_type = new PropertyType;
};

class VertexType() {
  @BeanProperty var type_id: Integer = 0;
  @BeanProperty var type_name: String = "";
  @BeanProperty var x_csr_params = new util.HashMap[String, String];
  @BeanProperty var properties = new java.util.ArrayList[Property];
  @BeanProperty var primary_keys = new java.util.ArrayList[String];
};

class VertexTypePairRelation() {
  @BeanProperty var source_vertex: String = "";
  @BeanProperty var destination_vertex: String = "";
  @BeanProperty var relation: String = "";
  @BeanProperty var x_csr_params = new util.HashMap[String, String];


  override def toString: String = {
    return "VertexTypePairRelation{src=" + source_vertex + ",dst=" + destination_vertex + ",relation=" + relation + "}";
  }

};

class EdgeType() {
  @BeanProperty var type_id: Integer = 0;
  @BeanProperty var type_name: String = "";
  @BeanProperty var vertex_type_pair_relations = new util.ArrayList[VertexTypePairRelation];
  @BeanProperty var properties = new util.ArrayList[Property]()

  override def toString: String = {
    return "EdgeType{type_id=" + type_id + ",type_name = " + type_name + vertex_type_pair_relations.toString + "}"
  }
}

class SchemaImpl() {
  private val LOG = LoggerFactory.getLogger(this.getClass);
  @BeanProperty var vertex_types = new java.util.ArrayList[VertexType];
  @BeanProperty var edge_types = new util.ArrayList[EdgeType];

  def getVertexPrimaryKey(vertexLabelName: String): util.ArrayList[String] = {
    vertex_types.forEach(vertex_type => {
      if (vertex_type.type_name.equals(vertexLabelName)) {
        return vertex_type.primary_keys;
      }
    })
    null
  }

  def getVertexProperties(labelName: String): util.ArrayList[Property] = {
    vertex_types.forEach(vertex_type => {
      if (vertex_type.type_name.equals(labelName)) {
        return vertex_type.properties;
      }
    })
    null
  }

  def getEdgeProperties(edgeLabelName: String, srcLabelName: String, dstLabelName: String): util.ArrayList[Property] = {
    edge_types.forEach(edge_type => {

      if (edge_type.type_name.equals(edgeLabelName)) {
        val vertexPairs = edge_type.vertex_type_pair_relations;
        vertexPairs.forEach(vertexPair => {
          LOG.info("query input {}({}->{}), cur {}({}->{})", edgeLabelName, srcLabelName, dstLabelName, edge_type.type_name, vertexPair.source_vertex, vertexPair.destination_vertex)
          if (vertexPair.source_vertex.equals(srcLabelName) && vertexPair.destination_vertex.equals(dstLabelName)) {
            LOG.info("Found")
            return edge_type.properties
          }
        })
      }
    })
    null
  }
};

class Schema() {

  @BeanProperty var name: String = "";
  @BeanProperty var store_type: String = "";
  @BeanProperty var schema = new SchemaImpl;

  def getVertexPrimaryKey(vertexLabelName: String): util.ArrayList[String] = {
    return schema.getVertexPrimaryKey(vertexLabelName);
  }

  def getVertexPrimaryKeyType(vertexLabelName: String): PropertyType = {
    val properties = getVertexProperties(vertexLabelName);
    val primary_keys = getVertexPrimaryKey(vertexLabelName);
    if (primary_keys.size() != 1){
      throw new RuntimeException("Expect only one primary key")
    }
    properties.forEach(property => {
      if (property.property_name.equals(primary_keys.get(0))) {
        return property.property_type
      }
    })
    throw new RuntimeException("Primary key not found " + vertexLabelName)
  }

  def getVertexProperties(vertexLabelName: String): util.ArrayList[Property] = {
    return schema.getVertexProperties(vertexLabelName);
  }

  def getEdgeProperties(edgeLabelName: String, srcLabel: String, dstLabel: String): util.ArrayList[Property] = {
    return schema.getEdgeProperties(edgeLabelName, srcLabel, dstLabel)
  }
}


object Schema {
  //Create a Schema Object from yaml file
  def LoadFromYaml(yamlPath: String, spark: SparkSession): Schema = {
    val path = new Path(yamlPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(
      new Constructor(classOf[Schema], new LoaderOptions())
    )
    yaml.load(input).asInstanceOf[Schema]
  }

  def LoadFromYaml(input : FSDataInputStream, spark : SparkSession): Schema = {
    val yaml = new Yaml(
      new Constructor(classOf[Schema], new LoaderOptions())
    )
    yaml.load(input).asInstanceOf[Schema]
  }
}