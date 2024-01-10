package com.alibaba.graphscope.interactive.reader

import com.alibaba.graphar.GarType
import com.alibaba.graphar.GarType.GarType
import com.alibaba.graphscope.interactive.reader.IReader.interactiveType2StructType
import com.alibaba.graphscope.interactive.{BulkLoadConfig, PropertyType, Schema}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util

trait IReader {
  /**
   * Read a source table into a data frame.
   * @param path path to the source, oss/odps/local are supported.
   * @return The data frame.
   */
  def Read() : DataFrame;
}

object IReader {
  def interactiveType2StructType(propertyType: PropertyType): GarType = {
    val primType = propertyType.primitive_type;
    if (primType.equals("DT_SIGNED_INT64")) {
      return GarType.INT64
    }
    else if (primType.equals("DT_SIGNED_INT32")) {
      return GarType.INT32
    }
    else if (primType.equals("DT_STRING")) {
      return GarType.STRING
    }
    else if (primType.equals("DT_STRINGMAP")) {
      return GarType.STRING // Store stringMap as string in GraphAr
    }
    else if (primType.equals("DT_DOUBLE")) {
      return GarType.DOUBLE
    }
    else if (primType.equals("DT_BOOL")) {
      return GarType.BOOL
    }
    else if (primType.equals("DT_UNSIGNED_INT8") || primType.equals("DT_SIGNED_INT8")){
      return GarType.INT32 // We store int8 as int32
    }
    else if (primType.equals("DT_UNSIGNED_INT16") || primType.equals("DT_SIGNED_INT16")){
      return GarType.INT32
    }
    else {
      throw new RuntimeException("Not supported type: " + propertyType.primitive_type)
    }
  }
}

trait IReaderFactory {
  def CreateVertexReader(labelName : String, path : String, sparkSession: SparkSession) : IReader;
  def CreateEdgeReader(labelName: String, srcLabel : String, dstLabel : String, path : String, sparkSession: SparkSession ) : IReader
}

object IReaderFactory {
  private val LOG = LoggerFactory.getLogger(this.getClass);
  def generateVertexSchema(schema : Schema, labelName: String, config: BulkLoadConfig): StructType = {
    val properties = schema.getVertexProperties(labelName)
    if (properties == null) {
      throw new RuntimeException("label: " + labelName + " not found in schema")
    }
    LOG.info("vertex {} properties: {}", labelName, properties.toString: Any)
    //TODO: Really use the column mapping
    val columnMappings = config.getVertexColumnMappings(labelName)
    if (columnMappings == null) {
      throw new RuntimeException("label: " + labelName + " not found in loading config")
    }
    var structTypeArray = new util.ArrayList[(String, GarType)]
    if (columnMappings.isEmpty) {
      //It is possible that the columnMappings is empty, which means the default mappings.
      //Currently we assume that the schema is correctly mapped to import.yaml
      for (i <- 0 until properties.size()) {
        structTypeArray.add(((properties.get(i).property_name), interactiveType2StructType(properties.get(i).property_type)))
      }
    }
    else {
      for (i <- 0 until columnMappings.size()) {
        val mapping = columnMappings.get(i)
        val propertyName = mapping.property
        val columnName = mapping.column.name
        if (propertyName == null || propertyName.isEmpty) {
          throw new RuntimeException("property name is empty")
        }
        if (columnName == null || columnName.isEmpty) {
          throw new RuntimeException("column name is empty")
        }
        val curProperty = properties.stream().filter(property => property.property_name.equals(propertyName)).findFirst().get()
        structTypeArray.add((
          columnName,
          interactiveType2StructType(curProperty.property_type),
        ))
      }
    }
    return generateStructType(structTypeArray)
  }

  def generateEdgeSchema(schema: Schema, labelName: String, srcLabel: String, dstLabel: String, config: BulkLoadConfig): StructType = {
    val properties = schema.getEdgeProperties(labelName, srcLabel, dstLabel)
    if (properties == null) {
      throw new RuntimeException("label: " + labelName + ", src: " + srcLabel + " dst: " + dstLabel + " not found in schema")
    }
    val columnMappings = config.getEdgeColumnMappings(labelName, srcLabel, dstLabel)
    val srcVertexMappings = config.getEdgeSrcVertexMappings(labelName, srcLabel, dstLabel)
    val dstVertexMappings = config.getEdgeDstVertexMappings(labelName,srcLabel, dstLabel)

    if (srcVertexMappings == null || srcVertexMappings.isEmpty){
      throw new RuntimeException("label: " + labelName + " srcVertexMappings is null or empty")
    }
    if (dstVertexMappings == null || dstVertexMappings.isEmpty){
      throw new RuntimeException("label: " + labelName + " dstVertexMappings is null or empty")
    }
    var structTypeArray = new util.ArrayList[(String, GarType)]
    // Src and dst vertex primary key must be set.
    assert(srcVertexMappings.size() == 1 && dstVertexMappings.size() == 1)
    structTypeArray.add((
      srcVertexMappings.get(0).column.name,
      interactiveType2StructType(schema.getVertexPrimaryKeyType(srcLabel)),
    ))
    //In case column name is the same, throw error
    if (srcVertexMappings.get(0).column.name.equals(dstVertexMappings.get(0).column.name)) {
      throw new RuntimeException("srcVertexMappings and dstVertexMappings have the same column name")
    }
    structTypeArray.add((
      dstVertexMappings.get(0).column.name,
      interactiveType2StructType(schema.getVertexPrimaryKeyType(dstLabel)),
    ))

    if (columnMappings == null) {
      throw new RuntimeException("label: " + labelName + " not found in loading config")
    }

    if (columnMappings.isEmpty) {
      //TODO: FIXME(zhanglei)
      for (i <- 0 until properties.size()) {
        structTypeArray.add(((properties.get(i).property_name), interactiveType2StructType(properties.get(i).property_type)))
      }
    }
    else {
      if (columnMappings.size() != properties.size()) {
        throw new RuntimeException("label: " + labelName + " columnMappings size: " + columnMappings.size() + " not equal to properties size: " + properties.size())
      }
      // add from columnMappings
      //First add the primary keys.

      // add from columnMappings
      columnMappings.forEach(mapping => {
        val curColName = mapping.column.name
        val curPropertyName = mapping.property
        val curProperty = properties.stream().filter(property => property.property_name.equals(curPropertyName)).findFirst().get()
        if (curProperty == null) {
          throw new RuntimeException("label: " + labelName + " property: " + curPropertyName + " not found in schema")
        }
        structTypeArray.add((
          curColName,
          interactiveType2StructType(curProperty.property_type),
        ))
      })
    }
    return generateStructType(structTypeArray)
  }

  def generateStructType(structTypeArray : util.ArrayList[(String, GarType)]): StructType = {
    var structType = new StructType()
    structTypeArray.forEach(pair => {
      if (pair._2 == GarType.BOOL) {
        structType = structType.add(pair._1, BooleanType, false)
      }
      else if (pair._2 == GarType.DOUBLE) {
        structType = structType.add(pair._1, DoubleType, false)
      }
      else if (pair._2 == GarType.INT64) {
        structType = structType.add(pair._1, LongType, false)
      }
      else if (pair._2 == GarType.INT32) {
        structType = structType.add(pair._1, IntegerType, false)
      }
      else if (pair._2 == GarType.STRING) {
        structType = structType.add(pair._1, StringType, false)
      }
    })
    return structType;
  }
}
