package com.alibaba.graphscope.interactive

import com.alibaba.graphar.GeneralParams
import com.alibaba.graphar.graph.GraphWriter
import com.alibaba.graphscope.interactive.reader.ReaderFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class BulkLoader(var sparkSession: SparkSession, var schema: Schema, var loadingConfig: BulkLoadConfig) {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  var isValid: Boolean = validate()
  if (!isValid) {
    System.err.println("Schema inconsistent with loadingConfig")
  }


  def validate(): Boolean = {
    //TODO: check the consistency between schema and loading config.
    return true;
  }

  def loadVertices(graphWriter: GraphWriter): Unit = {
    // For each vertices specified in loadingConfig, get the DataFrame, and write using writer
    var readerFactory = ReaderFactory.Create(loadingConfig ,schema)
    for (i <- 0 until  loadingConfig.vertex_mappings.size()) {
      val cur_vertex_mapping = loadingConfig.vertex_mappings.get(i);
      LOG.info("cur vertex mapping type name " + cur_vertex_mapping.type_name + ", inputs: " + cur_vertex_mapping.inputs.toString)
      var cur_frame: DataFrame = null
      for (j <- 0 until  cur_vertex_mapping.inputs.size()) {
        val loader = readerFactory.CreateVertexReader(cur_vertex_mapping.type_name, cur_vertex_mapping.inputs.get(j), sparkSession);
        if (cur_frame == null) {
          cur_frame = loader.Read();
          LOG.info("Got first frame {}", cur_frame.count())
        }
        else {
          val prevCount = cur_frame.count();
          cur_frame.union(loader.Read())
          LOG.info("Union frame from {} to {}", prevCount, cur_frame.count())
        }
      }

//      LOG.info("Got final frame size {}, {}", cur_frame.count(),cur_frame.collect().mkString("Array(", ", ", ")"))

      //Now call PutVertexData.
      val primaryKeys = schema.getVertexPrimaryKey(cur_vertex_mapping.type_name);
      if (primaryKeys == null) {
        throw new RuntimeException("Can not find type" + cur_vertex_mapping.type_name + " in schema")
      }
      if (primaryKeys.size() != 1) {
        throw new RuntimeException("Currently only support one primary key")
      }
      //Here the primary is not the primary key in schema, but the the corresponding column name in the DataFrame
      val schemaPrimaryKey = primaryKeys.get(0)
      var corrColumnName : String = null
      cur_vertex_mapping.column_mappings.forEach( mapping => {
        if (mapping.property.equals(schemaPrimaryKey)){
          corrColumnName = mapping.column.name
        }
      })
      if (corrColumnName == null) {
        throw new RuntimeException("Can not find primary key in column mapping")
      }
      graphWriter.PutVertexData(cur_vertex_mapping.type_name, cur_frame, corrColumnName)
      LOG.info("Finish putting vertex data for label {}, primary key {}", cur_vertex_mapping.type_name, primaryKeys.get(0): Any);
    }
  }

  def loadEdges(graphWriter: GraphWriter): Unit = {
    var readerFactory = ReaderFactory.Create(loadingConfig, schema)
    for (i <- 0 until loadingConfig.edge_mappings.size()) {
      val cur_edge_mapping = loadingConfig.edge_mappings.get(i);
      val cur_triplet = cur_edge_mapping.type_triplet
      LOG.info("cur vertex mapping type name " + cur_triplet + " inputs: " + cur_edge_mapping.inputs.toString);
      var cur_frame: DataFrame = null
      for (j <- 0 until  cur_edge_mapping.inputs.size()) {
        val loader = readerFactory.CreateEdgeReader(cur_triplet.edge, cur_triplet.source_vertex, cur_triplet.destination_vertex, cur_edge_mapping.inputs.get(j), sparkSession)
        if (cur_frame == null) {
          cur_frame = loader.Read();
          LOG.info("Got first frame {}", cur_frame.count())
        }
        else {
          val prevCount = cur_frame.count();
          cur_frame.union(loader.Read())
          LOG.info("Union frame from {} to {}", prevCount, cur_frame.count())
        }
      }
//      LOG.info("Got final frame size {} {}", cur_frame.count(), cur_frame.collect().mkString("Array(", ", ", ")"))

      val edgeTripletTuple = (cur_edge_mapping.type_triplet.source_vertex,
        cur_edge_mapping.type_triplet.edge,
        cur_edge_mapping.type_triplet.destination_vertex)
      graphWriter.PutEdgeData(edgeTripletTuple, cur_frame)
      LOG.info("Finish putting Edge data for edge {}", edgeTripletTuple)
    }
  }

  /**
   * Load Graph and deserialize to the specified output path. Could be oss.
   *
   * @param sparkSession
   * @return
   */
  def loadGraph(output_path: String): Boolean = {
    if (!isValid) {
      System.err.println("The Schema is inconsistent with loading config, can not load")
      return false
    }
    val writer = new GraphWriter()
    try {
      loadVertices(writer)
      loadEdges(writer)
      writer.write(output_path, sparkSession, schema.name, GeneralParams.defaultVertexChunkSize, GeneralParams.defaultEdgeChunkSize, file_type = "orc")
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
        return false;
    }
    return true;
  }
}

