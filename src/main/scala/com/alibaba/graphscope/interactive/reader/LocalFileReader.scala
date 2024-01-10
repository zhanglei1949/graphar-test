package com.alibaba.graphscope.interactive.reader

import com.alibaba.graphar.GarType
import com.alibaba.graphar.GarType.GarType
import com.alibaba.graphscope.interactive.{BulkLoadConfig, PropertyType, Schema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util

class CSVLocalFileReader(val path: String, val delimiter: String, val headerRow: Boolean,
                         val rootLocation: String, val schema: StructType, val sparkSession: SparkSession) extends IReader {
  /**
   * Read a source table into a data frame.
   *
   * @param path path to the source, oss/odps/local are supported.
   * @return The data frame.
   */
  override def Read(): DataFrame = {
    return sparkSession.read
      .option("delimiter", delimiter)
      .option("header", headerRow)
      .option("inferSchema", "false")
      .schema(schema)
      .csv(rootLocation + "/" + path).cache()
  }
}


//For local file Reader, we only support csv format.
class LocalFileReaderFactory(val loadingConfig: BulkLoadConfig, val schema: Schema) extends IReaderFactory {
  private val LOG = LoggerFactory.getLogger(this.getClass);
  var scheme: String = loadingConfig.loading_config.data_source.scheme;
  if (scheme.isEmpty) {
    scheme = "file"
  }
  var root_location: String = loadingConfig.loading_config.data_source.location;
  var format: String = loadingConfig.loading_config.format.`type`
  if (format.isEmpty) {
    format = "csv" //Default is csv
  }
  if (!format.equals("csv")) {
    throw new RuntimeException("Only support csv when reading from local file")
  }
  val delimiter: String = loadingConfig.loading_config.format.metadata.getOrDefault("delimiter", "|");
  val headRow: String = loadingConfig.loading_config.format.metadata.getOrDefault("header_row", "true")


  override def CreateVertexReader(labelName: String, path: String, spark: SparkSession): IReader = {
    val structType = generateSchema(labelName, loadingConfig)
    LOG.info("Vertex Table Schema: {}", structType.toString())
    if (headRow.equals("true") || headRow.equals("TRUE")) {
      return new CSVLocalFileReader(path, delimiter, true, root_location, structType, spark);
    }
    else {
      return new CSVLocalFileReader(path, delimiter, false, root_location, structType, spark);
    }
  }

  override def CreateEdgeReader(labelName: String, srcLabel: String, dstLabel: String, path: String, sparkSession: SparkSession): IReader = {
    val structType = generateEdgeSchema(labelName, srcLabel, dstLabel, loadingConfig)
    LOG.info("Edge Table Schema: {}", structType.toString())
    if (headRow.equals("true") || headRow.equals("TRUE")) {
      return new CSVLocalFileReader(path, delimiter, true, root_location, structType, sparkSession);
    }
    else {
      return new CSVLocalFileReader(path, delimiter, false, root_location, structType, sparkSession);
    }
  }

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
    else if (primType.equals("DT_DOUBLE")) {
      return GarType.DOUBLE
    }
    else if (primType.equals("DT_BOOL")) {
      return GarType.BOOL
    }
    else {
      throw new RuntimeException("Not supported type: " + propertyType.primitive_type)
    }
  }

  def generateSchema(labelName: String, config: BulkLoadConfig): StructType = {
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
    //It is possible that the columnMappings is empty, which means the default mappings.
    //Currently we assume that the schema is correctly mapped to import.yaml
    val structTypeArray = new util.ArrayList[(String, GarType)]
    for (i <- 0 until properties.size()) {
      structTypeArray.add(((properties.get(i).property_name), interactiveType2StructType(properties.get(i).property_type)))
    }

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

  def generateEdgeSchema(labelName: String, srcLabel: String, dstLabel: String, config: BulkLoadConfig): StructType = {
    val properties = schema.getEdgeProperties(labelName, srcLabel, dstLabel)
    if (properties == null) {
      throw new RuntimeException("label: " + labelName + ", src: " + srcLabel + " dst: " + dstLabel + " not found in schema")
    }
    val columnMappings = config.getEdgeColumnMappings(labelName, srcLabel, dstLabel)
    if (columnMappings == null) {
      throw new RuntimeException("label: " + labelName + " not found in loading config")
    }
    //TODO: Really use the column mapping
    //It is possible that the columnMappings is empty, which means the default mappings.
    //Currently we assume that the schema is correctly mapped to import.yaml
    val structTypeArray = new util.ArrayList[(String, GarType)]
    //First add the primary keys.
    structTypeArray.add((
      srcLabel + "." + schema.getVertexPrimaryKey(srcLabel),
      interactiveType2StructType(schema.getVertexPrimaryKeyType(srcLabel)),
    ))
    val dstLabelPriCol = {
      if (srcLabel.equals(dstLabel)) {
        dstLabel + "2" + "." + schema.getVertexPrimaryKey(dstLabel)
      }
      else {
        dstLabel + "." + schema.getVertexPrimaryKey(dstLabel)
      }
    }
    structTypeArray.add((
      dstLabelPriCol,
      interactiveType2StructType(schema.getVertexPrimaryKeyType(dstLabel)),
    ))

    //TODO: FIXME(zhanglei)
    for (i <- 0 until properties.size()) {
      structTypeArray.add(((properties.get(i).property_name), interactiveType2StructType(properties.get(i).property_type)))
    }

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