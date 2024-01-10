package com.alibaba.graphscope.interactive.reader

import com.alibaba.graphscope.interactive.{BulkLoadConfig, Schema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

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
    val structType = IReaderFactory.generateVertexSchema(schema, labelName, loadingConfig)
    LOG.info("Vertex Table Schema: {}", structType.toString())
    if (headRow.equals("true") || headRow.equals("TRUE")) {
      return new CSVLocalFileReader(path, delimiter, true, root_location, structType, spark);
    }
    else {
      return new CSVLocalFileReader(path, delimiter, false, root_location, structType, spark);
    }
  }

  override def CreateEdgeReader(labelName: String, srcLabel: String, dstLabel: String, path: String, sparkSession: SparkSession): IReader = {
    val structType = IReaderFactory.generateEdgeSchema(schema, labelName, srcLabel, dstLabel, loadingConfig)
    LOG.info("Edge Table Schema: {}", structType.toString())
    if (headRow.equals("true") || headRow.equals("TRUE")) {
      return new CSVLocalFileReader(path, delimiter, true, root_location, structType, sparkSession);
    }
    else {
      return new CSVLocalFileReader(path, delimiter, false, root_location, structType, sparkSession);
    }
  }
}