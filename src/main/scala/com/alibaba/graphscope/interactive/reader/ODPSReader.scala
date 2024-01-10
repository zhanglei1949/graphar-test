package com.alibaba.graphscope.interactive.reader

import com.alibaba.graphscope.interactive.reader.ODPSReader.tryParsePartitionFromPath
import com.alibaba.graphscope.interactive.{BulkLoadConfig, Schema}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class ODPSReader(val path: String, val rootLocation: String, val tableSchema: StructType, val sparkSession: SparkSession) extends IReader {
  private val LOG = LoggerFactory.getLogger(this.getClass);

  //  private val partition_name : String = ""
  //  private val partition : String = ""

  var fullPath: String = path;
  if (rootLocation.nonEmpty) {
    fullPath = rootLocation + "/" + fullPath
  }

  val (partition_name, partition, realPath) = tryParsePartitionFromPath(fullPath)
  LOG.info("Extract real path {} partition name {} and partition value {} from path {}", realPath, partition_name, partition, fullPath)

  /**
   * Read a source table into a data frame.
   *
   * @param path path to the source, oss/odps/local are supported.
   * @return The data frame.
   */
  override def Read(): DataFrame = {

    val sqlString = buildSqlString()
    LOG.info("Try to read table from {} with sql {}", realPath, sqlString: Any)
    val df = sparkSession.sql(sqlString).cache()
    LOG.info("Got df of size {}", df.count())
    return df
  }

  def buildSqlString(): String = {
    val sqlString = new StringBuilder()
    sqlString.append("select ")
    // append the column names
    val columnNames = tableSchema.fieldNames
    for (i <- columnNames.indices) {
      sqlString.append(columnNames(i))
      if (i < columnNames.length - 1) {
        sqlString.append(",")
      }
    }
    sqlString.append(" from ")
    sqlString.append(realPath)
    if (partition_name.nonEmpty && partition.nonEmpty) {
      sqlString.append(" where ")
      sqlString.append(partition_name)
      sqlString.append(" = ")
      sqlString.append(partition)
    }
    sqlString.toString()
  }
}

object ODPSReader {
  private val LOG = LoggerFactory.getLogger(this.getClass);

  /**
   * Parse the encoded partition info from string.
   *
   * @param fullPath The full path is like {project_name}/{table_name}/{partition_name}={partition_value}
   * @return
   */
  def tryParsePartitionFromPath(fullPath: String): (String, String, String) = {
    var partition_name = ""
    var partition = ""
    var realPath = ""
    val index = fullPath.lastIndexOf("/")
    if (index > 0) {
      val partitionStr = fullPath.substring(index + 1)
      val partitionIndex = partitionStr.indexOf("=")
      if (partitionIndex > 0) {
        partition_name = partitionStr.substring(0, partitionIndex)
        partition = partitionStr.substring(partitionIndex + 1)
        realPath = fullPath.substring(0, index)
      } else {
        realPath = fullPath
      }
    }
    //else, no partition info and project name
    LOG.info("after remove partition, real path is {}", realPath)
    //if realPath start with project name, remove it
    val realPathIndex = realPath.indexOf("/")
    if (realPathIndex > 0) {
      realPath = realPath.substring(realPathIndex + 1)
    }
    (partition_name, partition, realPath)
  }
}

class ODPSReaderFactory(var loadingConfig: BulkLoadConfig, val schema: Schema) extends IReaderFactory {
  private val LOG = LoggerFactory.getLogger(this.getClass);
  assert(loadingConfig.loading_config.data_source.scheme.equals("odps"))
  var root_location: String = loadingConfig.loading_config.data_source.location;
  var format: String = loadingConfig.loading_config.format.`type`
  if (format.isEmpty) {
    format = "arrow" //Default is csv
  }


  override def CreateVertexReader(labelName: String, path: String, sparkSession: SparkSession): IReader = {
    val tableSchema = IReaderFactory.generateVertexSchema(schema, labelName, loadingConfig)
    LOG.info("Vertex Table Schema: {}", tableSchema.toString())
    new ODPSReader(path, root_location, tableSchema, sparkSession);
  }

  override def CreateEdgeReader(labelName: String, srcLabel: String, dstLabel: String, path: String, sparkSession: SparkSession): IReader = {
    val tableSchema = IReaderFactory.generateEdgeSchema(schema,labelName, srcLabel, dstLabel , loadingConfig)
    LOG.info("Edge Table Schema: {}", tableSchema.toString())
    new ODPSReader(path, root_location, tableSchema, sparkSession);
  }
}
